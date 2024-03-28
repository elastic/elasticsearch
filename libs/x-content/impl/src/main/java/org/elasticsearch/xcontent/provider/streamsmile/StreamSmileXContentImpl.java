/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.provider.streamsmile;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.smile.SmileConstants;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.provider.smile.SmileXContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

/**
 * A Smile based content implementation using Jackson that escapes the stream separator.
 */
public final class StreamSmileXContentImpl implements XContent {

    public static XContentBuilder getContentBuilder() throws IOException {
        return XContentBuilder.builder(streamSmileXContent);
    }

    static final SmileFactory streamSmileFactory;
    private static final StreamSmileXContentImpl streamSmileXContent;

    public static XContent streamSmileXContent() {
        return streamSmileXContent;
    }

    static {
        streamSmileFactory = new SmileFactory();
        // Encode binary data in 7 bits so it can be safely streamed
        streamSmileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, true);
        streamSmileFactory.configure(SmileFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.dataformat.smile.SmileGenerator#close() method
        streamSmileFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        streamSmileFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        streamSmileFactory.configure(JsonParser.Feature.USE_FAST_DOUBLE_PARSER, true);
        streamSmileXContent = new StreamSmileXContentImpl();
    }

    private StreamSmileXContentImpl() {}

    @Override
    public XContentType type() {
        return XContentType.STREAM_SMILE;
    }

    @Override
    public byte streamSeparator() {
        return (byte) 0xFF;
    }

    @Override
    public boolean detectContent(byte[] bytes, int offset, int length) {
        return length > 2
            && bytes[offset] == SmileConstants.HEADER_BYTE_1
            && bytes[offset + 1] == SmileConstants.HEADER_BYTE_2
            && bytes[offset + 2] == SmileConstants.HEADER_BYTE_3
            && (bytes[offset + 3] & SmileConstants.HEADER_BIT_HAS_RAW_BINARY) != SmileConstants.HEADER_BIT_HAS_RAW_BINARY;
    }

    @Override
    public boolean detectContent(CharSequence chars) {
        return chars.length() > 2
            && chars.charAt(0) == SmileConstants.HEADER_BYTE_1
            && chars.charAt(1) == SmileConstants.HEADER_BYTE_2
            && chars.charAt(2) == SmileConstants.HEADER_BYTE_3
            && ((byte) chars.charAt(3) & SmileConstants.HEADER_BIT_HAS_RAW_BINARY) != SmileConstants.HEADER_BIT_HAS_RAW_BINARY;
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new StreamSmileXContentGenerator(streamSmileFactory.createGenerator(os, JsonEncoding.UTF8), os, includes, excludes);
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, String content) throws IOException {
        return new StreamSmileXContentParser(config, streamSmileFactory.createParser(content));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, InputStream is) throws IOException {
        return new StreamSmileXContentParser(config, streamSmileFactory.createParser(is));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, byte[] data, int offset, int length) throws IOException {
        return new StreamSmileXContentParser(config, streamSmileFactory.createParser(data, offset, length));
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration config, Reader reader) throws IOException {
        return new SmileXContentParser(config, streamSmileFactory.createParser(reader));
    }
}
