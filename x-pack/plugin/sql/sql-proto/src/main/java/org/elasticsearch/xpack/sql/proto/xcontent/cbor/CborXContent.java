/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent.cbor;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

import org.elasticsearch.xpack.sql.proto.xcontent.XContent;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentGenerator;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentParseException;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentParser;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;

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

    private CborXContent() {}

    @Override
    public XContentType type() {
        return XContentType.CBOR;
    }

    @Override
    public byte streamSeparator() {
        throw new XContentParseException("cbor does not support stream parsing...");
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os) throws IOException {
        return new CborXContentGenerator(cborFactory.createGenerator(os, JsonEncoding.UTF8), os);
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, String content) throws IOException {
        return new CborXContentParser(config, cborFactory.createParser(content));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, InputStream is) throws IOException {
        return new CborXContentParser(config, cborFactory.createParser(is));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, byte[] data, int offset, int length) throws IOException {
        return new CborXContentParser(config, cborFactory.createParser(data, offset, length));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, Reader reader) throws IOException {
        return new CborXContentParser(config, cborFactory.createParser(reader));
    }
}
