/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;

public class SearchTemplateHelper {
    private static final JsonFactory jsonLenientCommaFactory;

    static {
        jsonLenientCommaFactory = new JsonFactory();
        jsonLenientCommaFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonLenientCommaFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonLenientCommaFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false);
        jsonLenientCommaFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        jsonLenientCommaFactory.configure(JsonParser.Feature.ALLOW_TRAILING_COMMA, true);
    }

    /**
     * Accepts an invalid JSON string with trailing commas, parses it and prints it back without the trailing commas.
     * This is useful where a piece of JSON is generated from a template, but needs to be stripped of unnecessary
     * commas to be valid.
     *
     * @param jsonSource the JSON to clean
     * @return valid JSON
     * @throws IOException if the parsing fails, e.g. because the JSON is not well-formed
     */
    public static String stripTrailingComma(String jsonSource) throws IOException {
        JsonMapper mapper = new JsonMapper(jsonLenientCommaFactory);
        Object value = mapper.readValue(jsonSource, Object.class);
        return mapper.writeValueAsString(value);
    }
}
