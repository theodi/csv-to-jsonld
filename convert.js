const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const axios = require('axios');
const url = require('url');
const { Cipher } = require('crypto');

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

const globalAboutUrl = tableSchema.aboutUrl;

let labelData = [];
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
      const object = {};
      object[column.propertyUrl] = {
        '@id': column.propertyUrl,
        'rdfs:label': labels
      };
      labelData.push(object);
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
          for(const label of labelData) {
            results.push(label);
          }
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

function processValueUrl(valueUrl, row) {
  const placeholderCount = (valueUrl.match(/{(.+?)}/g) || []).length;
  if (placeholderCount == 0) {
    return valueUrl;
  }
  let replacement = "";
  const result = valueUrl.replace(/{(.+?)}/g, (_, key) => {
    replacement = row[key];
    return replacement;
  });

  if (replacement == "") {
    return replacement;
  } else {
    return result;
  }
  //return valueUrl.replace(/{(.+?)}/g, (_, key) => row[key]);
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

function updateNodeById(obj, aboutUrl, property, value) {
  if (obj["@id"] === aboutUrl) {
      // Found the node, add the property
      if (value.indexOf("/") > 0) {
        if (!obj[property]) {
          obj[property] = {};
        }
        obj[property]["@id"] = value;
      } else {
        obj[property] = value;
      }
      return true;
  }

  // If the current object is an object or array, recurse into it
  if (typeof obj === 'object' && obj !== null) {
      for (let key in obj) {
          if (obj.hasOwnProperty(key)) {
              let updated = updateNodeById(obj[key], aboutUrl, property, value);
              if (updated) {
                  return true;
              }
          }
      }
  }

  // Return false if the node wasn't found or updated
  return false;
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
        if (resolvedValueUrl !== "") {
          objectValue = { "@id": resolvedValueUrl };
        }
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
    const processedValue = processValueUrl(tableSchema.aboutUrl, internalRow);
    if (processedValue !== "") {
      result["@id"] = processedValue;
    }
  }

  // Add virtual columns
  tableSchema.columns.filter(col => col.virtual).forEach(virtualColumn => {
    if (!virtualColumn.aboutUrl) {
      let valueUrl = processValueUrl(virtualColumn.valueUrl,internalRow);
      if (valueUrl !== "") {
        result[virtualColumn.propertyUrl] = { "@id": valueUrl };
      }
    }
  });

  // Add properties to virtual columns
  tableSchema.columns.filter(col => col.virtual).forEach(virtualColumn => {
    if (virtualColumn.aboutUrl) {
      let aboutUrl = processValueUrl(virtualColumn.aboutUrl,internalRow);
      let valueUrl = processValueUrl(virtualColumn.valueUrl,internalRow);
      if (valueUrl !== "") {
        updateNodeById(result, aboutUrl, virtualColumn.propertyUrl, valueUrl);
      }
    }
  });

  return result;
}