/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.provider.XContentImplUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

/**
 * A JSON based content implementation using Jackson.
 */
public class JsonXContentImpl implements XContent {

    public static XContentBuilder getContentBuilder() throws IOException {
        return XContentBuilder.builder(jsonXContent);
    }

    private static final JsonFactory jsonFactory;

    public static final XContent jsonXContent;

    public static final XContent jsonXContent() {
        return jsonXContent;
    }

    static {
        jsonFactory = XContentImplUtils.configure(new JsonFactoryBuilder());
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.core.json.UTF8JsonGenerator#close() method
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        jsonFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        jsonFactory.configure(JsonParser.Feature.USE_FAST_DOUBLE_PARSER, true);
        // keeping existing behavior of including source, for now
        jsonFactory.configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true);
        jsonXContent = new JsonXContentImpl();
    }

    private JsonXContentImpl() {}

    @Override
    public XContentType type() {
        return XContentType.JSON;
    }

    @Override
    public byte bulkSeparator() {
        return '\n';
    }

    @Override
    public boolean detectContent(byte[] bytes, int offset, int length) {
        return bytes[offset] == '{';
    }

    @Override
    public boolean detectContent(CharSequence chars) {
        return chars.charAt(0) == '{';
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new JsonXContentGenerator(jsonFactory.createGenerator(os, JsonEncoding.UTF8), os, includes, excludes);
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, String content) throws IOException {
        return new JsonXContentParser(config, jsonFactory.createParser(content));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, InputStream is) throws IOException {
        return new JsonXContentParser(config, jsonFactory.createParser(is));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, byte[] data, int offset, int length) throws IOException {
        return new JsonXContentParser(config, jsonFactory.createParser(data, offset, length));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, Reader reader) throws IOException {
        return new JsonXContentParser(config, jsonFactory.createParser(reader));
    }
}
