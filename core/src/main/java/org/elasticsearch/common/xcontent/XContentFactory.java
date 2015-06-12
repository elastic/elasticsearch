/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.fasterxml.jackson.dataformat.cbor.CBORConstants;
import com.fasterxml.jackson.dataformat.smile.SmileConstants;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * A one stop to use {@link org.elasticsearch.common.xcontent.XContent} and {@link XContentBuilder}.
 */
public class XContentFactory {

    private static int GUESS_HEADER_LENGTH = 20;

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
     * Returns a content builder using YAML format ({@link org.elasticsearch.common.xcontent.XContentType#YAML}.
     */
    public static XContentBuilder yamlBuilder() throws IOException {
        return contentBuilder(XContentType.YAML);
    }

    /**
     * Constructs a new yaml builder that will output the result into the provided output stream.
     */
    public static XContentBuilder yamlBuilder(OutputStream os) throws IOException {
        return new XContentBuilder(YamlXContent.yamlXContent, os);
    }

    /**
     * Returns a content builder using CBOR format ({@link org.elasticsearch.common.xcontent.XContentType#CBOR}.
     */
    public static XContentBuilder cborBuilder() throws IOException {
        return contentBuilder(XContentType.CBOR);
    }

    /**
     * Constructs a new cbor builder that will output the result into the provided output stream.
     */
    public static XContentBuilder cborBuilder(OutputStream os) throws IOException {
        return new XContentBuilder(CborXContent.cborXContent, os);
    }

    /**
     * Constructs a xcontent builder that will output the result into the provided output stream.
     */
    public static XContentBuilder contentBuilder(XContentType type, OutputStream outputStream) throws IOException {
        if (type == XContentType.JSON) {
            return jsonBuilder(outputStream);
        } else if (type == XContentType.SMILE) {
            return smileBuilder(outputStream);
        } else if (type == XContentType.YAML) {
            return yamlBuilder(outputStream);
        } else if (type == XContentType.CBOR) {
            return cborBuilder(outputStream);
        }
        throw new IllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns a binary content builder for the provided content type.
     */
    public static XContentBuilder contentBuilder(XContentType type) throws IOException {
        if (type == XContentType.JSON) {
            return JsonXContent.contentBuilder();
        } else if (type == XContentType.SMILE) {
            return SmileXContent.contentBuilder();
        } else if (type == XContentType.YAML) {
            return YamlXContent.contentBuilder();
        } else if (type == XContentType.CBOR) {
            return CborXContent.contentBuilder();
        }
        throw new IllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns the {@link org.elasticsearch.common.xcontent.XContent} for the provided content type.
     */
    public static XContent xContent(XContentType type) {
        return type.xContent();
    }

    /**
     * Guesses the content type based on the provided char sequence.
     */
    public static XContentType xContentType(CharSequence content) {
        int length = content.length() < GUESS_HEADER_LENGTH ? content.length() : GUESS_HEADER_LENGTH;
        if (length == 0) {
            return null;
        }
        char first = content.charAt(0);
        if (first == '{') {
            return XContentType.JSON;
        }
        // Should we throw a failure here? Smile idea is to use it in bytes....
        if (length > 2 && first == SmileConstants.HEADER_BYTE_1 && content.charAt(1) == SmileConstants.HEADER_BYTE_2 && content.charAt(2) == SmileConstants.HEADER_BYTE_3) {
            return XContentType.SMILE;
        }
        if (length > 2 && first == '-' && content.charAt(1) == '-' && content.charAt(2) == '-') {
            return XContentType.YAML;
        }

        // CBOR is not supported

        for (int i = 0; i < length; i++) {
            char c = content.charAt(i);
            if (c == '{') {
                return XContentType.JSON;
            }
            if (Character.isWhitespace(c) == false) {
                break;
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
            throw new ElasticsearchParseException("Failed to derive xcontent");
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
            throw new ElasticsearchParseException("Failed to derive xcontent");
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
     * Guesses the content type based on the provided input stream without consuming it.
     */
    public static XContentType xContentType(InputStream si) throws IOException {
        if (si.markSupported() == false) {
            throw new IllegalArgumentException("Cannot guess the xcontent type without mark/reset support on " + si.getClass());
        }
        si.mark(GUESS_HEADER_LENGTH);
        try {
            final int firstInt = si.read(); // this must be an int since we need to respect the method contract
            if (firstInt == -1) {
                return null;
            }

            final int secondInt = si.read(); // this must be an int since we need to respect the method contract
            if (secondInt == -1) {
               return null;
            }
            final byte first = (byte) (0xff & firstInt);
            final byte second = (byte) (0xff & secondInt);
            if (first == SmileConstants.HEADER_BYTE_1 && second == SmileConstants.HEADER_BYTE_2) {
                int third = si.read();
                if (third == SmileConstants.HEADER_BYTE_3) {
                    return XContentType.SMILE;
                }
            }
            if (first == '{' || second == '{') {
                return XContentType.JSON;
            }
            if (first == '-' && second == '-') {
                int third = si.read();
                if (third == '-') {
                    return XContentType.YAML;
                }
            }
            // CBOR logic similar to CBORFactory#hasCBORFormat
            if (first == CBORConstants.BYTE_OBJECT_INDEFINITE){
                return XContentType.CBOR;
            }
            if (CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_TAG, first)) {
                // Actually, specific "self-describe tag" is a very good indicator
                int third = si.read();
                if (third == -1) {
                    return null;
                }
                if (first == (byte) 0xD9 && second == (byte) 0xD9 && third == (byte) 0xF7) {
                    return XContentType.CBOR;
                }
            }
            // for small objects, some encoders just encode as major type object, we can safely
            // say its CBOR since it doesn't contradict SMILE or JSON, and its a last resort
            if (CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, first)) {
                return XContentType.CBOR;
            }

            for (int i = 2; i < GUESS_HEADER_LENGTH; i++) {
                int val = si.read();
                if (val == -1) {
                    return null;
                }
                if (val == '{') {
                    return XContentType.JSON;
                }
                if (Character.isWhitespace(val) == false) {
                    break;
                }
            }
            return null;
        } finally {
            si.reset();
        }
    }

    /**
     * Guesses the content type based on the provided bytes.
     */
    public static XContentType xContentType(byte[] data, int offset, int length) {
        return xContentType(new BytesArray(data, offset, length));
    }

    public static XContent xContent(BytesReference bytes) {
        XContentType type = xContentType(bytes);
        if (type == null) {
            throw new ElasticsearchParseException("Failed to derive xcontent");
        }
        return xContent(type);
    }

    /**
     * Guesses the content type based on the provided bytes.
     */
    public static XContentType xContentType(BytesReference bytes) {
        int length = bytes.length();
        if (length == 0) {
            return null;
        }
        byte first = bytes.get(0);
        if (first == '{') {
            return XContentType.JSON;
        }
        if (length > 2 && first == SmileConstants.HEADER_BYTE_1 && bytes.get(1) == SmileConstants.HEADER_BYTE_2 && bytes.get(2) == SmileConstants.HEADER_BYTE_3) {
            return XContentType.SMILE;
        }
        if (length > 2 && first == '-' && bytes.get(1) == '-' && bytes.get(2) == '-') {
            return XContentType.YAML;
        }
        // CBOR logic similar to CBORFactory#hasCBORFormat
        if (first == CBORConstants.BYTE_OBJECT_INDEFINITE && length > 1){
            return XContentType.CBOR;
        }
        if (CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_TAG, first) && length > 2) {
            // Actually, specific "self-describe tag" is a very good indicator
            if (first == (byte) 0xD9 && bytes.get(1) == (byte) 0xD9 && bytes.get(2) == (byte) 0xF7) {
                return XContentType.CBOR;
            }
        }
        // for small objects, some encoders just encode as major type object, we can safely
        // say its CBOR since it doesn't contradict SMILE or JSON, and its a last resort
        if (CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, first)) {
            return XContentType.CBOR;
        }

        // a last chance for JSON
        for (int i = 0; i < length; i++) {
            byte b = bytes.get(i);
            if (b == '{') {
                return XContentType.JSON;
            }
            if (Character.isWhitespace(b) == false) {
                break;
            }
        }
        return null;
    }
}
