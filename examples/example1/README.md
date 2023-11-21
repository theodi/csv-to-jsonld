### README.md for Example1 CSVW Metadata

#### Overview
This README file describes the features of the CSVW metadata for the `example1` dataset, which comprises a list of financial transactions. The metadata is structured according to the CSV on the Web (CSVW) standard, facilitating the understanding and processing of the `transactions.csv` file.

#### CSVW Metadata Features

1. **Context Specification**:
   - The `@context` is set to the CSVW namespace with English as the default language.
   - This establishes the semantic context for interpreting the CSV data.

2. **Data Source**:
   - `url`: Points to the `transactions.csv` file, indicating the source of data.

3. **Table Schema**:
   - Defines the structure and constraints of the data in the CSV file.
   - Ensures consistency and facilitates data validation.

4. **Columns**:
   - **Transaction ID**:
     - `name`: "transaction_id"
     - `titles`: Multilingual titles provided, including English ("Transaction ID") and French ("ID de Transaction").
     - `datatype`: Integer, indicating a numeric identifier.
     - `required`: True, implying this column must have a value in each row.

   - **Date**:
     - `name`: "date"
     - `titles`: Provided in both English and French.
     - `datatype`: Date type with a specified range from January 1, 2023, to December 31, 2023.
     - `required`: True.

   - **Payer**:
     - `name`: "payer"
     - `titles`: Provided in both English ("Payer") and French ("Payeur").
     - `datatype`: String.
     - `required`: True.

   - **Payee**:
     - `name`: "payee"
     - `titles`: Provided in both English ("Payee") and French ("Bénéficiaire").
     - `datatype`: String.
     - `required`: True.

   - **Description**:
     - `name`: "description"
     - `titles`: "Description" (assumed to be the same in both English and French).
     - `datatype`: String.
     - `required`: True.

   - **Amount (USD)**:
     - `name`: "amount"
     - `titles`: Provided in both English and French, indicating the amount in USD.
     - `datatype`: Decimal, suitable for monetary values.
     - `required`: True.

5. **Primary Key**:
   - `primaryKey`: Set to "transaction_id", ensuring each transaction can be uniquely identified.

6. **About URL**:
   - `aboutUrl`: Provides a templated URL for each transaction, allowing the creation of unique identifiers for each row based on the `transaction_id`.

#### Usage
- This metadata file is used to parse, validate, and understand the `transactions.csv` file.
- It can be utilized by CSVW processing tools to generate RDF data or to facilitate data integration and mapping.

#### Limitations
- The metadata is tailored specifically for the `transactions.csv` file structure and may need adjustments for other datasets.
- Users should ensure compatibility with their CSVW processing tools.