const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const axios = require('axios');
const url = require('url');

if (process.argv.length <= 2) {
  console.error('Usage: node convert.js <metadata JSON file path>');
  process.exit(1);
}

const metadataFilePath = process.argv[2];
const metadata = require(path.resolve(metadataFilePath));

if (!metadata.url) {
  console.error('The metadata file does not contain a CSV URL or file path');
  process.exit(1);
}

const dataSource = metadata.url;

const tableSchema = metadata.tableSchema;

// Build a lookup table for column titles to internal names
const columns = {};
tableSchema.columns.forEach(column => {
  const titles = typeof column.titles === 'string' ? [column.titles] : Object.values(column.titles || {});
  titles.forEach(title => columns[title] = column.name);
});

const columnTitles = {};
tableSchema.columns.forEach(column => {
  const titles = typeof column.titles === 'string' ? column.titles : (column.titles || {});
  columnTitles[column.name] = titles;

  // Adding rdfs:label annotations for the properties
  if (column.propertyUrl) {
    let labels;
    if (typeof titles === 'string') {
      labels = [{'@value': titles, '@language': 'en'}];
    } else {
      labels = Object.keys(titles).map(lang => ({
        '@value': titles[lang],
        '@language': lang
      }));
    }
    if (labels.length > 0) {
      metadata[column.propertyUrl] = {
        '@id': column.propertyUrl,
        'rdfs:label': labels
      };
    }
  }
});

// Read the CSV file and convert each row
const results = [];

// Check if the dataSource is a URL or a local file path
if (url.parse(dataSource).protocol) {
  // dataSource is a URL
  axios({
    method: 'get',
    url: dataSource,
    responseType: 'stream'
  })
    .then(function (response) {
      response.data
        .pipe(csv())
        .on('data', (row) => {
          results.push(convertRowToJSONLD(row));
        })
        .on('end', () => {
          // Output the results
          const output = {
            "@context": metadata["@context"],
            "@graph": results
          };
          for (const key in metadata) {
            if (!['@context', 'url', 'tableSchema', 'dialect'].includes(key)) {
              output[key] = metadata[key];
            }
          }
          console.log(JSON.stringify(output, null, 2));
        });
    })
    .catch(function (error) {
      console.error('Error fetching the CSV file:', error);
    });
} else {
  // dataSource is a local file path
  fs.createReadStream(dataSource)
    .pipe(csv())
    .on('data', (row) => {
      results.push(convertRowToJSONLD(row));
    })
    .on('end', () => {
      // Output the results
      const output = {
        "@context": metadata["@context"],
        "@graph": results
      };
      for (const key in metadata) {
        if (!['@context', 'url', 'tableSchema', 'dialect'].includes(key)) {
          output[key] = metadata[key];
        }
      }
      console.log(JSON.stringify(output, null, 2));
    });
}

// Function to convert a CSV value based on its datatype
function convertType(value, type) {
  switch (type) {
    case 'integer':
      return parseInt(value);
    case 'decimal':
      return parseFloat(value);
    case 'date':
      return { "@value": value, "@type": "xsd:date" };
    default:
      return value;
  }
}

// Function to resolve valueUrl templates
function processValueUrl(valueUrl, row) {
  return valueUrl.replace(/{(.+?)}/g, (_, key) => row[key]);
}

// Function to convert a CSV value based on its datatype
function convertType(value, datatype) {
  if (datatype) {
    const baseType = datatype.base || datatype;
    switch (baseType) {
      case 'integer':
        return {"@value": parseInt(value), "@type": "xsd:integer"};
      case 'decimal':
        return {"@value": parseFloat(value), "@type": "xsd:decimal"};
      case 'date':
        return {"@value": value, "@type": "xsd:date"};
      // Add other datatypes as needed
    }
  }
  return value; // return as is if no datatype provided or not a recognized type
}

// Function to convert a CSV row to a JSON-LD object
function convertRowToJSONLD(row) {
  const result = {};
  const internalRow = {}; // This will hold the data in internal format
  const lookups = {};
  // Convert row data to internal format
  Object.entries(row).forEach(([columnTitle, value]) => {
    const columnName = columns[columnTitle];
    internalRow[columnName] = value;
  });

  // Process each column
  Object.entries(internalRow).forEach(([columnName, value]) => {
    const columnDefinition = tableSchema.columns.find(col => col.name === columnName);

    if (columnDefinition) {
      const propertyUrl = columnDefinition.propertyUrl || columnName;
      const datatype = columnDefinition.datatype;

      let objectValue;
      if (columnDefinition.valueUrl) {
        const resolvedValueUrl = processValueUrl(columnDefinition.valueUrl, internalRow);
        objectValue = { "@id": resolvedValueUrl };
      } else {
        objectValue = convertType(value, datatype);
      }
      if (!columnDefinition.suppressOutput) {
        result[propertyUrl] = objectValue;
      }
    }
  });

  // Add type information if it exists
  if (tableSchema.aboutUrl) {
    result["@id"] = processValueUrl(tableSchema.aboutUrl, internalRow);
  }

  // Add virtual columns
  tableSchema.columns.filter(col => col.virtual).forEach(virtualColumn => {
    if (!virtualColumn.aboutUrl) {
      let valueUrl = processValueUrl(virtualColumn.valueUrl,internalRow);
      result[virtualColumn.propertyUrl] = { "@id": valueUrl };
      lookups[valueUrl] = virtualColumn.propertyUrl;
    }
  });

  // Add properties to virtual columns
  tableSchema.columns.filter(col => col.virtual).forEach(virtualColumn => {
    if (virtualColumn.aboutUrl) {
      let aboutUrl = processValueUrl(virtualColumn.aboutUrl,internalRow);
      let valueUrl = processValueUrl(virtualColumn.valueUrl,internalRow);
      if (lookups[aboutUrl]) {
        result[lookups[aboutUrl]][virtualColumn.propertyUrl] = valueUrl;
      } else {
        result[virtualColumn.propertyUrl] = valueUrl;
      }
    }
  });

  return result;
}