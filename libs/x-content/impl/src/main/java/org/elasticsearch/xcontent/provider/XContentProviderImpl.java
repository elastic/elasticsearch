/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xcontent.provider.cbor.CborXContentImpl;
import org.elasticsearch.xcontent.provider.json.ESJsonFactoryBuilder;
import org.elasticsearch.xcontent.provider.json.JsonStringEncoderImpl;
import org.elasticsearch.xcontent.provider.json.JsonXContentImpl;
import org.elasticsearch.xcontent.provider.smile.SmileXContentImpl;
import org.elasticsearch.xcontent.provider.yaml.YamlXContentImpl;
import org.elasticsearch.xcontent.spi.SymbolTable;
import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.IOException;

public class XContentProviderImpl implements XContentProvider {

    public XContentProviderImpl() {}

    @Override
    public FormatProvider getCborXContent() {
        return new FormatProvider() {
            @Override
            public XContentBuilder getContentBuilder() throws IOException {
                return CborXContentImpl.getContentBuilder();
            }

            @Override
            public XContent XContent() {
                return CborXContentImpl.cborXContent();
            }
        };
    }

    @Override
    public FormatProvider getJsonXContent() {
        return new FormatProvider() {
            @Override
            public XContentBuilder getContentBuilder() throws IOException {
                return JsonXContentImpl.getContentBuilder();
            }

            @Override
            public XContent XContent() {
                return JsonXContentImpl.jsonXContent();
            }
        };
    }

    @Override
    public FormatProvider getSmileXContent() {
        return new FormatProvider() {
            @Override
            public XContentBuilder getContentBuilder() throws IOException {
                return SmileXContentImpl.getContentBuilder();
            }

            @Override
            public XContent XContent() {
                return SmileXContentImpl.smileXContent();
            }
        };
    }

    @Override
    public FormatProvider getYamlXContent() {
        return new FormatProvider() {
            @Override
            public XContentBuilder getContentBuilder() throws IOException {
                return YamlXContentImpl.getContentBuilder();
            }

            @Override
            public XContent XContent() {
                return YamlXContentImpl.yamlXContent();
            }
        };
    }

    @Override
    public XContentParserConfiguration empty() {
        return XContentParserConfigurationImpl.EMPTY;
    }

    @Override
    public JsonStringEncoder getJsonStringEncoder() {
        return JsonStringEncoderImpl.getInstance();
    }

    private static final int features;

    static {
        final JsonFactory jsonFactory;
        jsonFactory = XContentImplUtils.configure(new ESJsonFactoryBuilder());
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.core.json.UTF8JsonGenerator#close() method
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        jsonFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        jsonFactory.configure(JsonParser.Feature.USE_FAST_DOUBLE_PARSER, true);
        // keeping existing behavior of including source, for now
        jsonFactory.configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true);
        features = jsonFactory.getFactoryFeatures();
    }

    private final transient ByteQuadsCanonicalizer canonicalizer = ByteQuadsCanonicalizer.createRoot();

    private record ByteQuadsSymbolTable(ByteQuadsCanonicalizer child) implements SymbolTable {

        @Override
        public String findName(int[] quads, int qlen) {
            return child.findName(quads, qlen);
        }

        @Override
        public String addName(String newString, int[] quads, int qlen) {
            try {
                return child.addName(newString, quads, qlen);
            } catch (StreamConstraintsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            child.release();
        }
    }

    @Override
    public SymbolTable newSymbolTable() {
        ByteQuadsCanonicalizer child = canonicalizer.makeChild(features);
        return new ByteQuadsSymbolTable(child);
    }
}
