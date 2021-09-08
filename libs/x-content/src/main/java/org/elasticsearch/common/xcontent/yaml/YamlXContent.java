/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.yaml;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

/**
 * A YAML based content implementation using Jackson.
 */
public class YamlXContent implements XContent {

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(yamlXContent);
    }

    static final YAMLFactory yamlFactory;
    public static final YamlXContent yamlXContent;

    static {
        yamlFactory = new YAMLFactory();
        yamlFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        yamlXContent = new YamlXContent();
    }

    private YamlXContent() {
    }

    @Override
    public XContentType type() {
        return XContentType.YAML;
    }

    @Override
    public byte streamSeparator() {
        throw new UnsupportedOperationException("yaml does not support stream parsing...");
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new YamlXContentGenerator(yamlFactory.createGenerator(os, JsonEncoding.UTF8), os, includes, excludes);
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, String content) throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(content));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, InputStream is) throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(is));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, byte[] data) throws IOException {
        return createParser(xContentRegistry, deprecationHandler, data, 0, data.length);
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, byte[] data, int offset, int length) throws IOException {
        return createParserForCompatibility(xContentRegistry, deprecationHandler, data, offset, length, RestApiVersion.current());
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, Reader reader) throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(reader));
    }

    @Override
    public XContentParser createParserForCompatibility(NamedXContentRegistry xContentRegistry,
                                                       DeprecationHandler deprecationHandler, InputStream is,
                                                       RestApiVersion restApiVersion) throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(is), restApiVersion);
    }

    @Override
    public XContentParser createParserForCompatibility(NamedXContentRegistry xContentRegistry,
                                                       DeprecationHandler deprecationHandler, byte[] data, int offset, int length,
                                                       RestApiVersion restApiVersion) throws IOException {
        return new YamlXContentParser(
            xContentRegistry,
            deprecationHandler,
            yamlFactory.createParser(new ByteArrayInputStream(data, offset, length)),
            restApiVersion
        );
    }


}
