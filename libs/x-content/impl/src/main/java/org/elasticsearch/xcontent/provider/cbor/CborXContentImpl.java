/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.provider.cbor;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.cbor.CBORConstants;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParseException;
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
 * A CBOR based content implementation using Jackson.
 */
public final class CborXContentImpl implements XContent {

    public static XContentBuilder getContentBuilder() throws IOException {
        return XContentBuilder.builder(cborXContent);
    }

    static final CBORFactory cborFactory;
    private static final CborXContentImpl cborXContent;

    public static XContent cborXContent() {
        return cborXContent;
    }

    static {
        cborFactory = XContentImplUtils.configure(CBORFactory.builder());
        cborFactory.configure(CBORFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.dataformat.cbor.CBORGenerator#close() method
        cborFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        cborFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        cborFactory.configure(JsonParser.Feature.USE_FAST_DOUBLE_PARSER, true);
        cborXContent = new CborXContentImpl();
    }

    private CborXContentImpl() {}

    @Override
    public XContentType type() {
        return XContentType.CBOR;
    }

    @Override
    public byte bulkSeparator() {
        throw new XContentParseException("cbor does not support bulk parsing...");
    }

    @Override
    public boolean detectContent(byte[] bytes, int offset, int length) {
        // CBOR logic similar to CBORFactory#hasCBORFormat
        if (bytes[offset] == CBORConstants.BYTE_OBJECT_INDEFINITE && length > 1) {
            return true;
        }
        if (CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_TAG, bytes[offset]) && length > 2) {
            // Actually, specific "self-describe tag" is a very good indicator
            if (bytes[offset] == (byte) 0xD9 && bytes[offset + 1] == (byte) 0xD9 && bytes[offset + 2] == (byte) 0xF7) {
                return true;
            }
        }
        // for small objects, some encoders just encode as major type object, we can safely
        // say its CBOR since it doesn't contradict SMILE or JSON, and its a last resort
        if (CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, bytes[offset])) {
            return true;
        }
        return false;
    }

    @Override
    public boolean detectContent(CharSequence chars) {
        return false;
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new CborXContentGenerator(cborFactory.createGenerator(os, JsonEncoding.UTF8), os, includes, excludes);
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
