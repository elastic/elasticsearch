/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentEOFException;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;

class XContentRecordReader {
    static final int PARSE_ERRORS_LIMIT = 100;

    protected final XContentParser parser;
    protected final Map<String, Integer> fieldMap;
    protected final Logger logger;
    protected int nestedLevel;
    protected long fieldCount;
    protected int errorCounter;
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
    XContentRecordReader(XContentParser parser, Map<String, Integer> fieldMap, Logger logger) {
        this.parser = Objects.requireNonNull(parser);
        this.fieldMap = Objects.requireNonNull(fieldMap);
        this.logger = Objects.requireNonNull(logger);
    }

    /**
     * Read the JSON object and write to the record array. Nested objects are
     * flattened with the field names separated by a '.'. e.g. for a record with
     * a nested 'tags' object:
     * "{"name":"my.test.metric1","tags":{"tag1":"blah","tag2":"boo"},
     * "time":1350824400,"value":12345.678}" use 'tags.tag1' to reference the
     * tag1 field in the nested object
     * <p>
     * Array fields in the JSON are ignored
     *
     * @param record
     *            Read fields are written to this array. This array is first
     *            filled with empty strings and will never contain a
     *            <code>null</code>
     * @param gotFields
     *            boolean array each element is true if that field was read
     * @return The number of fields in the JSON doc or -1 if nothing was read
     *         because the end of the stream was reached
     */
    public long read(String[] record, boolean[] gotFields) throws IOException {
        initArrays(record, gotFields);
        fieldCount = 0;
        clearNestedLevel();

        XContentParser.Token token = tryNextTokenOrReadToEndOnError();
        while ((token == XContentParser.Token.END_OBJECT && nestedLevel == 0) == false) {
            if (token == null) {
                break;
            }

            if (token == XContentParser.Token.END_OBJECT) {
                --nestedLevel;
                String objectFieldName = nestedFields.pop();

                int lastIndex = nestedPrefix.length() - objectFieldName.length() - 1;
                nestedPrefix = nestedPrefix.substring(0, lastIndex);
            } else if (token == XContentParser.Token.FIELD_NAME) {
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

    protected void clearNestedLevel() {
        nestedLevel = 0;
        nestedFields = new ArrayDeque<String>();
        nestedPrefix = "";
    }

    private void parseFieldValuePair(String[] record, boolean[] gotFields) throws IOException {
        String fieldName = parser.currentName();
        XContentParser.Token token = tryNextTokenOrReadToEndOnError();

        if (token == null) {
            return;
        }

        if (token == XContentParser.Token.START_OBJECT) {
            ++nestedLevel;
            nestedFields.push(fieldName);
            nestedPrefix = nestedPrefix + fieldName + ".";
        } else {
            if (token == XContentParser.Token.START_ARRAY || token.isValue()) {
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

    private String parseSingleFieldValue(XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            // Convert any scalar values in the array to a comma delimited
            // string. (Arrays of more complex objects are ignored.)
            StringBuilder strBuilder = new StringBuilder();
            boolean needComma = false;
            while (token != XContentParser.Token.END_ARRAY) {
                token = tryNextTokenOrReadToEndOnError();
                if (token.isValue()) {
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

    private void skipSingleFieldValue(XContentParser.Token token) throws IOException {
        // Scalar values don't need any extra skip code
        if (token == XContentParser.Token.START_ARRAY) {
            // Consume the whole array but do nothing with it
            int arrayDepth = 1;
            do {
                token = tryNextTokenOrReadToEndOnError();
                if (token == XContentParser.Token.END_ARRAY) {
                    --arrayDepth;
                } else if (token == XContentParser.Token.START_ARRAY) {
                    ++arrayDepth;
                }
            } while (token != null && arrayDepth > 0);
        }
    }

    /**
     * Get the text representation of the current token unless it's a null.
     * Nulls are replaced with empty strings to match the way the C++ process
     * treats them (which, for historical interest, was originally shaped by
     * the fact that CSV cannot distinguish empty string and null).
     */
    private String tokenToString(XContentParser.Token token) throws IOException {
        if (token == null || token == XContentParser.Token.VALUE_NULL) {
            return "";
        }
        return parser.text();
    }

    protected void initArrays(String[] record, boolean[] gotFields) {
        Arrays.fill(gotFields, false);
        Arrays.fill(record, "");
    }

    /**
     * Returns null at the EOF or the next token
     */
    protected XContentParser.Token tryNextTokenOrReadToEndOnError() throws IOException {
        try {
            return parser.nextToken();
        } catch (XContentEOFException | XContentParseException e) {
            logger.warn("Attempting to recover from malformed JSON data.", e);
            for (int i = 0; i <= nestedLevel; ++i) {
                readToEndOfObject();
            }
            clearNestedLevel();
        }

        return parser.currentToken();
    }

    /**
     * In some cases the parser doesn't recognise the '}' of a badly formed JSON
     * document and so may skip to the end of the second document. In this case
     * we lose an extra record.
     */
    protected void readToEndOfObject() throws IOException {
        XContentParser.Token token = null;
        do {
            try {
                token = parser.nextToken();
            } catch (XContentEOFException | XContentParseException e) {
                ++errorCounter;
                if (errorCounter >= PARSE_ERRORS_LIMIT) {
                    logger.error("Failed to recover from malformed JSON data.", e);
                    throw new ElasticsearchParseException("The input JSON data is malformed.");
                }
            }
        } while (token != XContentParser.Token.END_OBJECT);
    }
}
