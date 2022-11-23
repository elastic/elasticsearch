/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.provider;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xcontent.provider.cbor.CborXContentImpl;
import org.elasticsearch.xcontent.provider.json.JsonStringEncoderImpl;
import org.elasticsearch.xcontent.provider.json.JsonXContentImpl;
import org.elasticsearch.xcontent.provider.smile.SmileXContentImpl;
import org.elasticsearch.xcontent.provider.yaml.YamlXContentImpl;
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
}
