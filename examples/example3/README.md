### README.md for Example 3 CSVW Metadata

#### Overview
This README discusses the CSVW metadata for `example3`, which represents a dataset of financial transactions. While retaining the core structure of `example2`, `example3` introduces additional complexities and refinements in its metadata configuration.

#### Key Differences from Example 2

1. **Extended `@context`**:
   - **Base URL**: The `@base` is set to `"http://example.com"`, which provides a base URI for relative URLs.
   - **Additional Namespace**: Introduction of a new namespace `"pay"` mapped to `"http://reference.data.gov.uk/def/payment#"`. This enables the use of `pay` prefix for property URLs.

2. **Table Schema Enhancements**:
   - **Property URLs**: Unlike `example2`, `example3` specifies `propertyUrl` for most columns, providing more explicit semantic context. For instance:
     - `schema:identifier` for `transaction_id`.
     - `schema:date` for `date`.
     - Custom URIs like `payer` and `payee` for respective columns.
   - **Suppress Output**: The `amount` column has a new attribute `"suppressOutput": true`, which indicates that this column's data should not be directly included in the output.

3. **Virtual Columns for Complex Structures**:
   - Introduction of multiple virtual columns to construct a `MonetaryAmount` object:
     - A virtual column for `schema:amount` creates a link to a detailed amount description.
     - Further virtual columns define a `MonetaryAmount` structure with `rdf:type`, `schema:value`, and `schema:currency` properties.
   - These virtual columns leverage the `aboutUrl` and `valueUrl` attributes to create more complex data structures than in `example2`.

4. **Refined Semantic Descriptions**:
   - The use of `propertyUrl` and virtual columns allows for a richer semantic representation of the data, adhering more closely to standards like Schema.org and RDF.

#### Usage
- The metadata in `example3` is designed to provide a more semantically rich and detailed description of the CSV data. It is particularly useful for scenarios requiring advanced data integration or RDF conversion.
- The metadata in `example3` not only provides a detailed semantic description of the CSV data but also marks the first instance in our examples that will generate a JSON-LD file fully compatible with JSON-LD tools, such as the [JSON-LD Playground](https://json-ld.org/playground/). This compatibility ensures that users can validate, visualize, and interact with the transformed JSON-LD data using standard JSON-LD processors and tools.

#### Limitations
- The complexity of the virtual column setup in `example3` might require more sophisticated processing logic compared to simpler CSVW metadata.
- Users should ensure their processing tools can handle the advanced features used in this metadata.