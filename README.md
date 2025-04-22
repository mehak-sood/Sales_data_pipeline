# Sales_data_pipeline

An automated ETL (Extract, Transform, Load) pipeline designed to transfer data from an AWS S3 bucket to a MySQL database.
This project streamlines data ingestion, ensuring efficient and reliable data flow for downstream applications.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Overview

This pipeline automates the process of:

1. **Extraction**: Fetching data files from a specified AWS S3 bucket.
2. **Transformation**: Processing and cleaning the data to match the desired schema and quality standards.
3. **Loading**: Inserting the transformed data into a MySQL database for storage and further analysis.

The modular design ensures scalability and ease of maintenance, making it suitable for various data engineering tasks.

## Features

- **Automated Workflow**: Seamless integration between data extraction, transformation, and loading processes.
- **Modular Design**: Clear separation of concerns, allowing for easy updates and maintenance.
- **Error Handling**: Robust mechanisms to handle exceptions and ensure data integrity.
- **Logging**: Comprehensive logs for monitoring and debugging purposes.

## Project Structure

```
DE_ETL_pipeline/
├── .idea/                  # IDE configuration files
├── docs/                   # Documentation and related resources
├── resources/              # Sample data files and configurations
├── src/                    # Source code for ETL processes
│   ├── main.py             # Script to extract data from S3 and load to MySQL
├── .gitattributes          # Git attributes configuration
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

## Prerequisites

Before setting up the project, ensure you have the following installed:

- **Python 3.6 or higher**
- **MySQL Server**
- **AWS CLI** (configured with appropriate access credentials)
- **pip** (Python package installer)

## Installation

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/mehak-sood/DE_ETL_pipeline.git
   cd DE_ETL_pipeline
   ```

2. **Create a Virtual Environment** (optional but recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Configure AWS Credentials**:

   Ensure that your AWS CLI is configured with the necessary permissions to access the S3 bucket:

   ```bash
   aws configure
   ```

2. **Set Up Configuration**:

   Update the configuration files in the `resources/` directory with your specific parameters, such as S3 bucket name, MySQL connection details, and data file paths.

3. **Run the ETL Pipeline**:

   Execute the main script to start the ETL process:

   ```bash
   python src/main.py
   ```

   This script will sequentially run the extraction, transformation, and loading steps.

## Configuration

The pipeline uses configuration files located in the `resources/` directory. These files specify parameters such as:

- **S3 Bucket Details**: Name of the bucket, file paths, and access credentials.
- **MySQL Connection**: Host, port, username, password, and database name.
- **Data Schema**: Expected structure of the data for validation purposes.

Ensure that these configurations are set correctly before running the pipeline.

## Contributing

Contributions are welcome!
If you have suggestions for improvements or find any issues, please open an issue or submit a pull request.
For major changes, it's advisable to discuss them first to ensure alignment with the project's goals.

## License

This project is licensed under the [MIT License](LICENSE).

For more details and updates, visit the [project repository](https://github.com/mehak-sood/DE_ETL_pipeline/).
