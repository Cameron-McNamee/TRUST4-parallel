#!/bin/bash

# Define variables
BUCKET_NAME="sra-source-large"
human_tcrbcr="$HOME/TRUST4/hg38_bcrtcr.fa"
human_ref="$HOME/TRUST4/human_IMGT+C.fa"
LOCAL_DIR="$HOME/trust4_processing"
OUTPUT_DIR="$LOCAL_DIR/output"

PARALLEL_JOBS=5       # Adjust based on your EC2 instance's capacity
download_limit=4102     # Set the number of files to process for testing

# Create necessary directories
mkdir -p "$LOCAL_DIR"
mkdir -p "$OUTPUT_DIR"

is_processed() {
    local accession_id="$1"
    if aws s3 ls "s3://$BUCKET_NAME/reports/${accession_id}_report.tsv" > /dev/null 2>&1; then
        return 0  # Accession ID has been processed
    else
        return 1  # Accession ID has not been processed
    fi
}
# Function to process a file
process_file() {
    local sra_file=$1
    local base_name=$(basename "$sra_file")
    local accession_id="${base_name%%.*}"
    local fastq_dir="$LOCAL_DIR/${accession_id}_fastq"
    mkdir -p "$fastq_dir"

    echo "Processing $accession_id"

    # Convert SRA to FASTQ
    echo "Converting $sra_file to FASTQ..."
    fasterq-dump "$sra_file" -O "$fastq_dir" --split-files --threads 20

    # Check if conversion was successful
    if [ $? -ne 0 ]; then
        echo "Error converting $sra_file to FASTQ."
        rm -rf "$fastq_dir"
        rm -f "$sra_file"
        return 1
    fi

    # Run TRUST4 on the FASTQ files
    echo "Running TRUST4 on $accession_id..."
    TRUST4/run-trust4 -f "$human_tcrbcr" --ref "$human_ref" \
        -1 "$fastq_dir/${accession_id}_1.fastq" \
        -2 "$fastq_dir/${accession_id}_2.fastq" \
        -t 20 --od "$OUTPUT_DIR" -o "$accession_id"

    # Check if TRUST4 ran successfully
    if [ $? -ne 0 ]; then
        echo "TRUST4 failed on $accession_id."
        rm -rf "$fastq_dir"
        rm -f "$sra_file"
        return 1
    fi

    # Upload the results and clean up
    echo "Uploading results for $accession_id..."
    for output_file in "$OUTPUT_DIR"/*; do
        if [[ $output_file == *"_annot.fa" ]]; then
            aws s3 cp "$output_file" "s3://$BUCKET_NAME/annotations/"
            rm -f "$output_file"
        elif [[ $output_file == *"_report.tsv" ]]; then
            aws s3 cp "$output_file" "s3://$BUCKET_NAME/reports/"
            rm -f "$output_file"
        fi
    done

    # Clean up local files
   rm -rf "$fastq_dir"
    rm -f "$sra_file"

    echo "Completed processing $accession_id"
}

export -f process_file
export BUCKET_NAME human_tcrbcr human_ref LOCAL_DIR OUTPUT_DIR

# Get the list of SRA files and limit the number
echo "Fetching list of SRA files from S3 bucket..."
FILES=$(aws s3 ls "s3://$BUCKET_NAME/sra/" --recursive | grep -v '/$' | awk '{print $4}' | head -n "$download_limit")

if [ -z "$FILES" ]; then
    echo "No files found in the S3 bucket."
    exit 1
fi

batch=()
for file in $FILES; do
   accession_id=$(basename "$file")
   accession_id="${accession_id%%.*}"
   echo "Beginninng $accession_id..."
    if is_processed "$accession_id"; then
        echo "Skipping $file as it has already been processed..."
        continue
    fi

    local_filename="$LOCAL_DIR/$(basename "$file")"
    echo "Downloading $file to $local_filename..."
    aws s3 cp "s3://$BUCKET_NAME/$file" "$local_filename"

    # Check if download was successful
    if [ $? -ne 0 ]; then
        echo "Failed to download $file."
        continue
    fi

    batch+=("$local_filename")

    # Process files in batches
    if [[ ${#batch[@]} -eq $PARALLEL_JOBS ]]; then
        printf "%s\n" "${batch[@]}" | parallel -j "$PARALLEL_JOBS" process_file
        echo "Cleaning up directories after batch processing..."
        rm -rf "$LOCAL_DIR"/*
        rm -rf "$OUTPUT_DIR"/*

        batch=()  # Reset batch
    fi
done
# Process any remaining files
if [[ ${#batch[@]} -gt 0 ]]; then
    printf "%s\n" "${batch[@]}" | parallel -j "$PARALLEL_JOBS" process_file
    echo "Cleaning up directories after batch processing..."
    rm -rf "$LOCAL_DIR"/*
    rm -rf "$OUTPUT_DIR"/*
fi

echo "Processing complete."