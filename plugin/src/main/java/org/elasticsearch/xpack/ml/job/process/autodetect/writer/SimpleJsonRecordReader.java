/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

class SimpleJsonRecordReader extends AbstractJsonRecordReader {
    private Deque<String> nestedFields;
    private String nestedPrefix;

    /**
     * Create a reader that parses the mapped fields from JSON.
     *
     * @param parser
     *            The JSON parser
     * @param fieldMap
     *            Map to field name to record array index position
     * @param logger
     *            logger
     */
    SimpleJsonRecordReader(JsonParser parser, Map<String, Integer> fieldMap, Logger logger) {
        super(parser, fieldMap, logger);
    }

    /**
     * Read the JSON object and write to the record array.
     * Nested objects are flattened with the field names separated by
     * a '.'.
     * e.g. for a record with a nested 'tags' object:
     * "{"name":"my.test.metric1","tags":{"tag1":"blah","tag2":"boo"},"time":1350824400,"value":12345.678}"
     * use 'tags.tag1' to reference the tag1 field in the nested object
     * <p>
     * Array fields in the JSON are ignored
     *
     * @param record    Read fields are written to this array. This array is first filled with empty
     *                  strings and will never contain a <code>null</code>
     * @param gotFields boolean array each element is true if that field
     *                  was read
     * @return The number of fields in the JSON doc or -1 if nothing was read
     * because the end of the stream was reached
     */
    @Override
    public long read(String[] record, boolean[] gotFields) throws IOException {
        initArrays(record, gotFields);
        fieldCount = 0;
        clearNestedLevel();

        JsonToken token = tryNextTokenOrReadToEndOnError();
        while (!(token == JsonToken.END_OBJECT && nestedLevel == 0)) {
            if (token == null) {
                break;
            }

            if (token == JsonToken.END_OBJECT) {
                --nestedLevel;
                String objectFieldName = nestedFields.pop();

                int lastIndex = nestedPrefix.length() - objectFieldName.length() - 1;
                nestedPrefix = nestedPrefix.substring(0, lastIndex);
            } else if (token == JsonToken.FIELD_NAME) {
                parseFieldValuePair(record, gotFields);
            }

            token = tryNextTokenOrReadToEndOnError();
        }

        // null token means EOF
        if (token == null) {
            return -1;
        }
        return fieldCount;
    }

    @Override
    protected void clearNestedLevel() {
        nestedLevel = 0;
        nestedFields = new ArrayDeque<String>();
        nestedPrefix = "";
    }

    private void parseFieldValuePair(String[] record, boolean[] gotFields) throws IOException {
        String fieldName = parser.getCurrentName();
        JsonToken token = tryNextTokenOrReadToEndOnError();

        if (token == null) {
            return;
        }

        if (token == JsonToken.START_OBJECT) {
            ++nestedLevel;
            nestedFields.push(fieldName);
            nestedPrefix = nestedPrefix + fieldName + ".";
        } else {
            if (token == JsonToken.START_ARRAY || token.isScalarValue()) {
                ++fieldCount;

                // Only do the donkey work of converting the field value to a
                // string if we need it
                Integer index = fieldMap.get(nestedPrefix + fieldName);
                if (index != null) {
                    record[index] = parseSingleFieldValue(token);
                    gotFields[index] = true;
                } else {
                    skipSingleFieldValue(token);
                }
            }
        }
    }

    private String parseSingleFieldValue(JsonToken token) throws IOException {
        if (token == JsonToken.START_ARRAY) {
            // Convert any scalar values in the array to a comma delimited
            // string.  (Arrays of more complex objects are ignored.)
            StringBuilder strBuilder = new StringBuilder();
            boolean needComma = false;
            while (token != JsonToken.END_ARRAY) {
                token = tryNextTokenOrReadToEndOnError();
                if (token.isScalarValue()) {
                    if (needComma) {
                        strBuilder.append(',');
                    } else {
                        needComma = true;
                    }
                    strBuilder.append(tokenToString(token));
                }
            }

            return strBuilder.toString();
        }

        return tokenToString(token);
    }

    private void skipSingleFieldValue(JsonToken token) throws IOException {
        // Scalar values don't need any extra skip code
        if (token == JsonToken.START_ARRAY) {
            // Consume the whole array but do nothing with it
            int arrayDepth = 1;
            do {
                token = tryNextTokenOrReadToEndOnError();
                if (token == JsonToken.END_ARRAY) {
                    --arrayDepth;
                } else if (token == JsonToken.START_ARRAY) {
                    ++arrayDepth;
                }
            }
            while (token != null && arrayDepth > 0);
        }
    }

    /**
     * Get the text representation of the current token unless it's a null.
     * Nulls are replaced with empty strings to match the way the rest of the
     * product treats them (which in turn is shaped by the fact that CSV
     * cannot distinguish empty string and null).
     */
    private String tokenToString(JsonToken token) throws IOException {
        if (token == null || token == JsonToken.VALUE_NULL) {
            return "";
        }
        return parser.getText();
    }
}
