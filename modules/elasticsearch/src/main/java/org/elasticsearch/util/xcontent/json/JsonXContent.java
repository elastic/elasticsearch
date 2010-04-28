/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.xcontent.json;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.util.ThreadLocals;
import org.elasticsearch.util.io.FastStringReader;
import org.elasticsearch.util.xcontent.XContent;
import org.elasticsearch.util.xcontent.XContentGenerator;
import org.elasticsearch.util.xcontent.XContentParser;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;
import org.elasticsearch.util.xcontent.builder.TextXContentBuilder;

import java.io.*;

/**
 * @author kimchy (shay.banon)
 */
public class JsonXContent implements XContent {

    public static class CachedBinaryBuilder {

        private static final ThreadLocal<ThreadLocals.CleanableValue<BinaryXContentBuilder>> cache = new ThreadLocal<ThreadLocals.CleanableValue<BinaryXContentBuilder>>() {
            @Override protected ThreadLocals.CleanableValue<BinaryXContentBuilder> initialValue() {
                try {
                    BinaryXContentBuilder builder = new BinaryXContentBuilder(new JsonXContent());
                    return new ThreadLocals.CleanableValue<BinaryXContentBuilder>(builder);
                } catch (IOException e) {
                    throw new ElasticSearchException("Failed to create json generator", e);
                }
            }
        };

        /**
         * Returns the cached thread local generator, with its internal {@link StringBuilder} cleared.
         */
        static BinaryXContentBuilder cached() throws IOException {
            ThreadLocals.CleanableValue<BinaryXContentBuilder> cached = cache.get();
            cached.get().reset();
            return cached.get();
        }
    }

    public static class CachedTextBuilder {

        private static final ThreadLocal<ThreadLocals.CleanableValue<TextXContentBuilder>> cache = new ThreadLocal<ThreadLocals.CleanableValue<TextXContentBuilder>>() {
            @Override protected ThreadLocals.CleanableValue<TextXContentBuilder> initialValue() {
                try {
                    TextXContentBuilder builder = new TextXContentBuilder(new JsonXContent());
                    return new ThreadLocals.CleanableValue<TextXContentBuilder>(builder);
                } catch (IOException e) {
                    throw new ElasticSearchException("Failed to create json generator", e);
                }
            }
        };

        /**
         * Returns the cached thread local generator, with its internal {@link StringBuilder} cleared.
         */
        static TextXContentBuilder cached() throws IOException {
            ThreadLocals.CleanableValue<TextXContentBuilder> cached = cache.get();
            cached.get().reset();
            return cached.get();
        }
    }

    public static BinaryXContentBuilder contentBuilder() throws IOException {
        return contentBinaryBuilder();
    }

    public static BinaryXContentBuilder contentBinaryBuilder() throws IOException {
        return CachedBinaryBuilder.cached();
    }

    public static TextXContentBuilder contentTextBuilder() throws IOException {
        return CachedTextBuilder.cached();
    }


    private final JsonFactory jsonFactory;

    public JsonXContent() {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
    }

    @Override public XContentType type() {
        return XContentType.JSON;
    }

    @Override public XContentGenerator createGenerator(OutputStream os) throws IOException {
        return new JsonXContentGenerator(jsonFactory.createJsonGenerator(os, JsonEncoding.UTF8));
    }

    @Override public XContentGenerator createGenerator(Writer writer) throws IOException {
        return new JsonXContentGenerator(jsonFactory.createJsonGenerator(writer));
    }

    @Override public XContentParser createParser(String content) throws IOException {
        return new JsonXContentParser(jsonFactory.createJsonParser(new FastStringReader(content)));
    }

    @Override public XContentParser createParser(InputStream is) throws IOException {
        return new JsonXContentParser(jsonFactory.createJsonParser(is));
    }

    @Override public XContentParser createParser(byte[] data) throws IOException {
        return new JsonXContentParser(jsonFactory.createJsonParser(data));
    }

    @Override public XContentParser createParser(byte[] data, int offset, int length) throws IOException {
        return new JsonXContentParser(jsonFactory.createJsonParser(data, offset, length));
    }

    @Override public XContentParser createParser(Reader reader) throws IOException {
        return new JsonXContentParser(jsonFactory.createJsonParser(reader));
    }
}
