/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.xcontent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class WatcherXContentUtils {

    private WatcherXContentUtils() {
    }

    public static Tuple<XContentType, Object> convertToObject(BytesReference bytes) throws ElasticsearchParseException {
        if (bytes.hasArray()) {
            return convertToObject(bytes.array(), bytes.arrayOffset(), bytes.length());
        }
        try {
            XContentParser parser;
            XContentType contentType;
            Compressor compressor = CompressorFactory.compressor(bytes);
            if (compressor != null) {
                CompressedStreamInput compressedStreamInput = compressor.streamInput(bytes.streamInput());
                contentType = XContentFactory.xContentType(compressedStreamInput);
                compressedStreamInput.resetToBufferStart();
                parser = XContentFactory.xContent(contentType).createParser(compressedStreamInput);
            } else {
                contentType = XContentFactory.xContentType(bytes);
                parser = XContentFactory.xContent(contentType).createParser(bytes.streamInput());
            }
            return Tuple.tuple(contentType, readValue(parser, parser.nextToken()));
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    public static Tuple<XContentType, Object> convertToObject(byte[] data, int offset, int length) throws ElasticsearchParseException {
        try {
            XContentParser parser;
            XContentType contentType;
            Compressor compressor = CompressorFactory.compressor(data, offset, length);
            if (compressor != null) {
                CompressedStreamInput compressedStreamInput = compressor.streamInput(new BytesStreamInput(data, offset, length, false));
                contentType = XContentFactory.xContentType(compressedStreamInput);
                compressedStreamInput.resetToBufferStart();
                parser = XContentFactory.xContent(contentType).createParser(compressedStreamInput);
            } else {
                contentType = XContentFactory.xContentType(data, offset, length);
                parser = XContentFactory.xContent(contentType).createParser(data, offset, length);
            }
            return Tuple.tuple(contentType, readValue(parser, parser.nextToken()));
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    // TODO open this up in core
    public static List<Object> readList(XContentParser parser, XContentParser.Token token) throws IOException {
        List<Object> list = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            list.add(readValue(parser, token));
        }
        return list;
    }

    // TODO open this up in core
    public static Object readValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = parser.numberType();
            if (numberType == XContentParser.NumberType.INT) {
                return parser.intValue();
            } else if (numberType == XContentParser.NumberType.LONG) {
                return parser.longValue();
            } else if (numberType == XContentParser.NumberType.FLOAT) {
                return parser.floatValue();
            } else if (numberType == XContentParser.NumberType.DOUBLE) {
                return parser.doubleValue();
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue();
        } else if (token == XContentParser.Token.START_OBJECT) {
            return parser.map();
        } else if (token == XContentParser.Token.START_ARRAY) {
            return readList(parser, token);
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            return parser.binaryValue();
        }
        return null;
    }
}
