/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

abstract class AbstractJsonRecordReader implements JsonRecordReader {
    static final int PARSE_ERRORS_LIMIT = 100;

    // NORELEASE - Remove direct dependency on Jackson
    protected final JsonParser parser;
    protected final Map<String, Integer> fieldMap;
    protected final Logger logger;
    protected int nestedLevel;
    protected long fieldCount;
    protected int errorCounter;

    /**
     * Create a reader that parses the mapped fields from JSON.
     *
     * @param parser
     *            The JSON parser
     * @param fieldMap
     *            Map to field name to record array index position
     * @param logger
     *            the logger
     */
    AbstractJsonRecordReader(JsonParser parser, Map<String, Integer> fieldMap, Logger logger) {
        this.parser = Objects.requireNonNull(parser);
        this.fieldMap = Objects.requireNonNull(fieldMap);
        this.logger = Objects.requireNonNull(logger);
    }

    protected void initArrays(String[] record, boolean[] gotFields) {
        Arrays.fill(gotFields, false);
        Arrays.fill(record, "");
    }

    /**
     * Returns null at the EOF or the next token
     */
    protected JsonToken tryNextTokenOrReadToEndOnError() throws IOException {
        try {
            return parser.nextToken();
        } catch (JsonParseException e) {
            logger.warn("Attempting to recover from malformed JSON data.", e);
            for (int i = 0; i <= nestedLevel; ++i) {
                readToEndOfObject();
            }
            clearNestedLevel();
        }

        return parser.getCurrentToken();
    }

    protected abstract void clearNestedLevel();

    /**
     * In some cases the parser doesn't recognise the '}' of a badly formed
     * JSON document and so may skip to the end of the second document. In this
     * case we lose an extra record.
     */
    protected void readToEndOfObject() throws IOException {
        JsonToken token = null;
        do {
            try {
                token = parser.nextToken();
            } catch (JsonParseException e) {
                ++errorCounter;
                if (errorCounter >= PARSE_ERRORS_LIMIT) {
                    logger.error("Failed to recover from malformed JSON data.", e);
                    throw new ElasticsearchParseException("The input JSON data is malformed.");
                }
            }
        }
        while (token != JsonToken.END_OBJECT);
    }
}
