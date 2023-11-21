### README.md for CSV-to-JSONLD Conversion Tool

#### Overview
"csv-to-jsonld" is a Node.js tool designed for converting CSV files into JSON-LD format, utilizing CSVW metadata. It is especially useful for datasets like financial transactions between companies, showcased in the example files provided.

#### Requirements
- Node.js

#### Installation
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/theodi/csv-to-jsonld.git
   ```

2. **Install Dependencies**:
   Navigate to the project directory and run:
   ```bash
   npm install
   ```

#### Usage
To run the script, execute the following command:
```bash
npm convert.js <metadata JSON file path>
```
- `<metadata JSON file path>` should be the path to your CSVW metadata JSON file.

#### Example Files
The examples directory contains a series of examples which all build in complexity as a demonstration. Each has a README file.

#### Features
- Handles CSV data from URLs and local files.
- Uses CSVW metadata for JSON-LD conversion.
- Supports virtual columns and complex JSON-LD structures.
- Adapts to specified data types in the metadata.

#### Script Structure
1. **Metadata Processing**: Loads CSVW metadata.
2. **Data Source Reading**: Fetches CSV data from the specified source.
3. **Conversion**: Transforms CSV rows into JSON-LD format.
4. **Output Generation**: Compiles the converted data into JSON-LD.

#### Key Functions
- `convertType`: Adapts values to the specified data types.
- `processValueUrl`: Handles template URLs in metadata.
- `convertRowToJSONLD`: Converts individual CSV rows into JSON-LD objects.

#### Limitations
- **Conformance with CSVW Specification**: This tool has not been exhaustively tested for full compliance with the CSVW specification and should be used primarily as a reference or starting point.
- **Context Definition**: `csv-to-jsonld` accepts metadata with additional namespaces defined. For example:

  ```json
  "@context": [
    "http://www.w3.org/ns/csvw",
    {
     "@language": "en",
     "pay": "http://reference.data.gov.uk/def/payment#"
    }
  ]
  ```

  However, be aware that the CSVW validator at [https://csvw.opendata.cz](https://csvw.opendata.cz) may report this as invalid, even though it is a legitimate configuration. The validator typically accepts only basic context definitions like:

  ```json
  "@context": [
    "http://www.w3.org/ns/csvw",
    {"@language": "en"}
  ]
  ```

#### Debugging
The script includes console logs for tracking the process and identifying potential issues.

#### Contributing
Contributions are welcome. Please maintain standard coding practices and thoroughly document any additions or changes.

---

This README provides a comprehensive overview of the "csv-to-jsonld" tool, including its usage, features, and limitations, particularly regarding the acceptance of additional namespaces in the CSVW context. Make sure to update repository URLs and other details as necessary to match your project specifics.