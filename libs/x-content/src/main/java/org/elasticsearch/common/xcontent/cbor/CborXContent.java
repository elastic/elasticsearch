/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.cbor;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

/**
 * A CBOR based content implementation using Jackson.
 */
public class CborXContent implements XContent {

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(cborXContent);
    }

    static final CBORFactory cborFactory;
    public static final CborXContent cborXContent;

    static {
        cborFactory = new CBORFactory();
        cborFactory.configure(CBORFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.dataformat.cbor.CBORGenerator#close() method
        cborFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        cborFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        cborXContent = new CborXContent();
    }

    private CborXContent() {
    }

    @Override
    public XContentType type() {
        return XContentType.CBOR;
    }

    @Override
    public byte streamSeparator() {
        throw new XContentParseException("cbor does not support stream parsing...");
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new CborXContentGenerator(cborFactory.createGenerator(os, JsonEncoding.UTF8), os, includes, excludes);
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, String content) throws IOException {
        return new CborXContentParser(xContentRegistry, deprecationHandler, cborFactory.createParser(content));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, InputStream is) throws IOException {
        return new CborXContentParser(xContentRegistry, deprecationHandler, cborFactory.createParser(is));
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
        return new CborXContentParser(xContentRegistry, deprecationHandler, cborFactory.createParser(reader));
    }

    @Override
    public XContentParser createParserForCompatibility(NamedXContentRegistry xContentRegistry,
                                                       DeprecationHandler deprecationHandler, InputStream is,
                                                       RestApiVersion restApiVersion)
        throws IOException {
        return new CborXContentParser(xContentRegistry, deprecationHandler, cborFactory.createParser(is), restApiVersion);
    }

    @Override
    public XContentParser createParserForCompatibility(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler,
                                                       byte[] data, int offset, int length, RestApiVersion restApiVersion)
        throws IOException {
        return new CborXContentParser(
            xContentRegistry,
            deprecationHandler,
            cborFactory.createParser(new ByteArrayInputStream(data, offset, length)),
            restApiVersion
        );
    }

}
