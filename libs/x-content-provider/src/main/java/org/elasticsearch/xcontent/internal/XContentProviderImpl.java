/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import com.fasterxml.jackson.dataformat.smile.SmileConstants;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.internal.cbor.CborXContentImpl;
import org.elasticsearch.xcontent.internal.json.JsonXContentImpl;
import org.elasticsearch.xcontent.internal.smile.SmileXContentImpl;
import org.elasticsearch.xcontent.internal.yaml.YamlXContentImpl;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;

public class XContentProviderImpl implements XContentProvider {

    public XContentProviderImpl() {}

    @Override
    public FormatProvider<CborXContent> getCborXContent() {
        return new FormatProvider<>() {
            @Override
            public XContentBuilder getContentBuilder() throws IOException {
                return CborXContentImpl.getContentBuilder();
            }

            @Override
            public CborXContent XContent() {
                return CborXContentImpl.cborXContent();
            }
        };
    }

    @Override
    public FormatProvider<JsonXContent> getJsonXContent() {
        return new FormatProvider<>() {
            @Override
            public XContentBuilder getContentBuilder() throws IOException {
                return JsonXContentImpl.getContentBuilder();
            }

            @Override
            public JsonXContent XContent() {
                return JsonXContentImpl.jsonXContent();
            }
        };
    }

    @Override
    public FormatProvider<SmileXContent> getSmileXContent() {
        return new FormatProvider<>() {
            @Override
            public XContentBuilder getContentBuilder() throws IOException {
                return SmileXContentImpl.getContentBuilder();
            }

            @Override
            public SmileXContent XContent() {
                return SmileXContentImpl.smileXContent();
            }
        };
    }

    @Override
    public FormatProvider<YamlXContent> getYamlXContent() {
        return new FormatProvider<>() {
            @Override
            public XContentBuilder getContentBuilder() throws IOException {
                return SmileXContentImpl.getContentBuilder();
            }

            @Override
            public YamlXContent XContent() {
                return YamlXContentImpl.yamlXContent();
            }
        };
    }

    @Override
    public XContentParserConfiguration empty() {
        return XContentParserConfigurationImpl.EMPTY;
    }
}
