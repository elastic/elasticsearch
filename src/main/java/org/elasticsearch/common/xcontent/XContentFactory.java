/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.xcontent;

import com.fasterxml.jackson.dataformat.smile.SmileConstants;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * A one stop to use {@link org.elasticsearch.common.xcontent.XContent} and {@link XContentBuilder}.
 */
public class XContentFactory {

    private static int GUESS_HEADER_LENGTH = 20;

    private static final XContent[] contents;

    static {
        contents = new XContent[2];
        contents[0] = JsonXContent.jsonXContent;
        contents[1] = SmileXContent.smileXContent;
    }

    /**
     * Returns a content builder using JSON format ({@link org.elasticsearch.common.xcontent.XContentType#JSON}.
     */
    public static XContentBuilder jsonBuilder() throws IOException {
        return contentBuilder(XContentType.JSON);
    }

    /**
     * Constructs a new json builder that will output the result into the provided output stream.
     */
    public static XContentBuilder jsonBuilder(OutputStream os) throws IOException {
        return new XContentBuilder(JsonXContent.jsonXContent, os);
    }

    /**
     * Returns a content builder using SMILE format ({@link org.elasticsearch.common.xcontent.XContentType#SMILE}.
     */
    public static XContentBuilder smileBuilder() throws IOException {
        return contentBuilder(XContentType.SMILE);
    }

    /**
     * Constructs a new json builder that will output the result into the provided output stream.
     */
    public static XContentBuilder smileBuilder(OutputStream os) throws IOException {
        return new XContentBuilder(SmileXContent.smileXContent, os);
    }

    /**
     * Constructs a xcontent builder that will output the result into the provided output stream.
     */
    public static XContentBuilder contentBuilder(XContentType type, OutputStream outputStream) throws IOException {
        if (type == XContentType.JSON) {
            return jsonBuilder(outputStream);
        } else if (type == XContentType.SMILE) {
            return smileBuilder(outputStream);
        }
        throw new ElasticSearchIllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns a binary content builder for the provided content type.
     */
    public static XContentBuilder contentBuilder(XContentType type) throws IOException {
        if (type == XContentType.JSON) {
            return JsonXContent.contentBuilder();
        } else if (type == XContentType.SMILE) {
            return SmileXContent.contentBuilder();
        }
        throw new ElasticSearchIllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns the {@link org.elasticsearch.common.xcontent.XContent} for the provided content type.
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
            throw new ElasticSearchParseException("Failed to derive xcontent from (offset=" + offset + ", length=" + length + "): " + Arrays.toString(data));
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
     * Guesses the content type based on the provided input stream.
     */
    public static XContentType xContentType(InputStream si) throws IOException {
        int first = si.read();
        if (first == -1) {
            return null;
        }
        int second = si.read();
        if (second == -1) {
            return null;
        }
        if (first == SmileConstants.HEADER_BYTE_1 && second == SmileConstants.HEADER_BYTE_2) {
            int third = si.read();
            if (third == SmileConstants.HEADER_BYTE_3) {
                return XContentType.SMILE;
            }
        }
        if (first == '{' || second == '{') {
            return XContentType.JSON;
        }
        for (int i = 2; i < GUESS_HEADER_LENGTH; i++) {
            int val = si.read();
            if (val == -1) {
                return null;
            }
            if (val == '{') {
                return XContentType.JSON;
            }
        }
        return null;
    }

    /**
     * Guesses the content type based on the provided bytes.
     */
    public static XContentType xContentType(byte[] data, int offset, int length) {
        length = length < GUESS_HEADER_LENGTH ? length : GUESS_HEADER_LENGTH;
        if (length > 2 && data[offset] == SmileConstants.HEADER_BYTE_1 && data[offset + 1] == SmileConstants.HEADER_BYTE_2 && data[offset + 2] == SmileConstants.HEADER_BYTE_3) {
            return XContentType.SMILE;
        }
        int size = offset + length;
        for (int i = offset; i < size; i++) {
            if (data[i] == '{') {
                return XContentType.JSON;
            }
        }
        return null;
    }

    public static XContent xContent(BytesReference bytes) {
        XContentType type = xContentType(bytes);
        if (type == null) {
            throw new ElasticSearchParseException("Failed to derive xcontent from " + bytes);
        }
        return xContent(type);
    }

    /**
     * Guesses the content type based on the provided bytes.
     */
    public static XContentType xContentType(BytesReference bytes) {
        int length = bytes.length() < GUESS_HEADER_LENGTH ? bytes.length() : GUESS_HEADER_LENGTH;
        if (length > 2 && bytes.get(0) == SmileConstants.HEADER_BYTE_1 && bytes.get(1) == SmileConstants.HEADER_BYTE_2 && bytes.get(2) == SmileConstants.HEADER_BYTE_3) {
            return XContentType.SMILE;
        }
        for (int i = 0; i < length; i++) {
            if (bytes.get(i) == '{') {
                return XContentType.JSON;
            }
        }
        return null;
    }
}
