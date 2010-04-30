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

package org.elasticsearch.util.xcontent;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;
import org.elasticsearch.util.xcontent.builder.TextXContentBuilder;
import org.elasticsearch.util.xcontent.json.JsonXContent;
import org.elasticsearch.util.xcontent.xson.XsonXContent;

import java.io.IOException;
import java.util.Arrays;

/**
 * A one stop to use {@link org.elasticsearch.util.xcontent.XContent} and {@link org.elasticsearch.util.xcontent.builder.XContentBuilder}.
 *
 * @author kimchy (shay.banon)
 */
public class XContentFactory {

    private static int GUESS_HEADER_LENGTH = 20;

    private static final XContent[] contents;

    static {
        contents = new XContent[2];
        contents[0] = new JsonXContent();
        contents[1] = new XsonXContent();
    }

    /**
     * Returns a binary content builder using JSON format ({@link org.elasticsearch.util.xcontent.XContentType#JSON}.
     */
    public static BinaryXContentBuilder jsonBuilder() throws IOException {
        return contentBinaryBuilder(XContentType.JSON);
    }

    /**
     * Returns a binary content builder using XSON format ({@link org.elasticsearch.util.xcontent.XContentType#XSON}.
     */
    public static BinaryXContentBuilder xsonBuilder() throws IOException {
        return contentBinaryBuilder(XContentType.XSON);
    }

    /**
     * Returns a binary content builder for the provided content type.
     */
    public static BinaryXContentBuilder contentBuilder(XContentType type) throws IOException {
        if (type == XContentType.JSON) {
            return JsonXContent.contentBinaryBuilder();
        } else if (type == XContentType.XSON) {
            return XsonXContent.contentBinaryBuilder();
        }
        throw new ElasticSearchIllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns a binary content builder for the provided content type.
     */
    public static BinaryXContentBuilder contentBinaryBuilder(XContentType type) throws IOException {
        if (type == XContentType.JSON) {
            return JsonXContent.contentBinaryBuilder();
        } else if (type == XContentType.XSON) {
            return XsonXContent.contentBinaryBuilder();
        }
        throw new ElasticSearchIllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns a textual content builder for the provided content type. Note, XSON does not support this... .
     */
    public static TextXContentBuilder contentTextBuilder(XContentType type) throws IOException {
        if (type == XContentType.JSON) {
            return JsonXContent.contentTextBuilder();
        }
        throw new ElasticSearchIllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns the {@link org.elasticsearch.util.xcontent.XContent} for the provided content type.
     */
    public static XContent xContent(XContentType type) {
        return contents[type.index()];
    }

    /**
     * Guesses the content type based on the provided char sequence.
     */
    public static XContentType xContentType(CharSequence content) {
        int length = content.length() < GUESS_HEADER_LENGTH ? content.length() : GUESS_HEADER_LENGTH;
        for (int i = 0; i < length; i++) {
            char c = content.charAt(i);
            if (c == '{') {
                return XContentType.JSON;
            }
        }
        return null;
    }

    /**
     * Guesses the content (type) based on the provided char sequence.
     */
    public static XContent xContent(CharSequence content) {
        XContentType type = xContentType(content);
        if (type == null) {
            throw new ElasticSearchParseException("Failed to derive xcontent from " + content);
        }
        return xContent(type);
    }

    /**
     * Guesses the content type based on the provided bytes.
     */
    public static XContent xContent(byte[] data) {
        return xContent(data, 0, data.length);
    }

    /**
     * Guesses the content type based on the provided bytes.
     */
    public static XContent xContent(byte[] data, int offset, int length) {
        XContentType type = xContentType(data, offset, length);
        if (type == null) {
            throw new ElasticSearchParseException("Failed to derive xcontent from " + Arrays.toString(data));
        }
        return xContent(type);
    }

    /**
     * Guesses the content type based on the provided bytes.
     */
    public static XContentType xContentType(byte[] data) {
        return xContentType(data, 0, data.length);
    }

    /**
     * Guesses the content type based on the provided bytes.
     */
    public static XContentType xContentType(byte[] data, int offset, int length) {
        length = length < GUESS_HEADER_LENGTH ? length : GUESS_HEADER_LENGTH;
        if (length > 1 && data[0] == 0x00 && data[1] == 0x00) {
            return XContentType.XSON;
        }
        for (int i = offset; i < length; i++) {
            if (data[i] == '{') {
                return XContentType.JSON;
            }
        }
        return null;
    }
}
