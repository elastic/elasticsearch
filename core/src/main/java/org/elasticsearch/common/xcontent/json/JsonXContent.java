/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;

/**
 * A JSON based content implementation using Jackson.
 */
public class JsonXContent implements XContent {

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(jsonXContent);
    }

    private final static JsonFactory jsonFactory;
    public final static String JSON_ALLOW_UNQUOTED_FIELD_NAMES = "elasticsearch.json.allow_unquoted_field_names";
    public final static JsonXContent jsonXContent;
    public final static boolean unquotedFieldNamesSet;

    static {
        jsonFactory = new JsonFactory();
        // TODO: Remove the system property configuration for this in Elasticsearch 6.0.0
        String jsonUnquoteProp = System.getProperty(JSON_ALLOW_UNQUOTED_FIELD_NAMES);
        if (jsonUnquoteProp == null) {
            unquotedFieldNamesSet = false;
            jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, false);
        } else {
            unquotedFieldNamesSet = true;
            switch (jsonUnquoteProp) {
                case "true":
                    jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
                    break;
                case "false":
                    jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, false);
                    break;
                default:
                    throw new IllegalArgumentException("invalid value for [" + JSON_ALLOW_UNQUOTED_FIELD_NAMES + "]: " + jsonUnquoteProp);
            }
        }
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.core.json.UTF8JsonGenerator#close() method
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        jsonXContent = new JsonXContent();
    }

    private JsonXContent() {
    }

    @Override
    public XContentType type() {
        return XContentType.JSON;
    }

    @Override
    public byte streamSeparator() {
        return '\n';
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, String[] filters, boolean inclusive) throws IOException {
        return new JsonXContentGenerator(jsonFactory.createGenerator(os, JsonEncoding.UTF8), os, filters, inclusive);
    }

    @Override
    public XContentParser createParser(String content) throws IOException {
        return new JsonXContentParser(jsonFactory.createParser(new FastStringReader(content)));
    }

    @Override
    public XContentParser createParser(InputStream is) throws IOException {
        return new JsonXContentParser(jsonFactory.createParser(is));
    }

    @Override
    public XContentParser createParser(byte[] data) throws IOException {
        return new JsonXContentParser(jsonFactory.createParser(data));
    }

    @Override
    public XContentParser createParser(byte[] data, int offset, int length) throws IOException {
        return new JsonXContentParser(jsonFactory.createParser(data, offset, length));
    }

    @Override
    public XContentParser createParser(BytesReference bytes) throws IOException {
        if (bytes.hasArray()) {
            return createParser(bytes.array(), bytes.arrayOffset(), bytes.length());
        }
        return createParser(bytes.streamInput());
    }

    @Override
    public XContentParser createParser(Reader reader) throws IOException {
        return new JsonXContentParser(jsonFactory.createParser(reader));
    }
}
