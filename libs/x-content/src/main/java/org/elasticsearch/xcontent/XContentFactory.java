/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A one stop to use {@link org.elasticsearch.xcontent.XContent} and {@link XContentBuilder}.
 */
public class XContentFactory {

    public static final int GUESS_HEADER_LENGTH = 20;

    private XContentFactory() {}

    /**
     * Returns a content builder using JSON format ({@link org.elasticsearch.xcontent.XContentType#JSON}.
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
     * Returns a content builder using SMILE format ({@link org.elasticsearch.xcontent.XContentType#SMILE}.
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
     * Returns a content builder using YAML format ({@link org.elasticsearch.xcontent.XContentType#YAML}.
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
     * Returns a content builder using CBOR format ({@link org.elasticsearch.xcontent.XContentType#CBOR}.
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
        XContentType canonical = type.canonical();
        if (canonical == XContentType.JSON) {
            return jsonBuilder(outputStream);
        } else if (canonical == XContentType.SMILE) {
            return smileBuilder(outputStream);
        } else if (canonical == XContentType.YAML) {
            return yamlBuilder(outputStream);
        } else if (canonical == XContentType.CBOR) {
            return cborBuilder(outputStream);
        }
        throw new IllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns a binary content builder for the provided content type.
     */
    public static XContentBuilder contentBuilder(XContentType type) throws IOException {
        XContentType canonical = type.canonical();

        if (canonical == XContentType.JSON) {
            return JsonXContent.contentBuilder();
        } else if (canonical == XContentType.SMILE) {
            return SmileXContent.contentBuilder();
        } else if (canonical == XContentType.YAML) {
            return YamlXContent.contentBuilder();
        } else if (canonical == XContentType.CBOR) {
            return CborXContent.contentBuilder();
        }
        throw new IllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Returns the {@link org.elasticsearch.xcontent.XContent} for the provided content type.
     */
    public static XContent xContent(XContentType type) {
        if (type == null) {
            throw new IllegalArgumentException("Cannot get xcontent for unknown type");
        }
        return type.xContent();
    }

    /**
     * Guesses the content type based on the provided char sequence.
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static XContentType xContentType(CharSequence content) {
        int length = content.length() < GUESS_HEADER_LENGTH ? content.length() : GUESS_HEADER_LENGTH;
        if (length == 0) {
            return null;
        }
        char first = content.charAt(0);
        if (JsonXContent.jsonXContent.detectContent(content)) {
            return XContentType.JSON;
        }
        // Should we throw a failure here? Smile idea is to use it in bytes....
        if (SmileXContent.smileXContent.detectContent(content)) {
            return XContentType.SMILE;
        }
        if (YamlXContent.yamlXContent.detectContent(content)) {
            return XContentType.YAML;
        }
        // CBOR is not supported

        // fallback for JSON
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
     * Guesses the content (type) based on the provided char sequence and returns the corresponding {@link XContent}
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static XContent xContent(CharSequence content) {
        XContentType type = xContentType(content);
        if (type == null) {
            throw new XContentParseException("Failed to derive xcontent");
        }
        return xContent(type);
    }

    /**
     * Guesses the content type based on the provided bytes and returns the corresponding {@link XContent}
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static XContent xContent(byte[] data) {
        return xContent(data, 0, data.length);
    }

    /**
     * Guesses the content type based on the provided bytes and returns the corresponding {@link XContent}
     *
     * @deprecated guessing the content type should not be needed ideally. We should rather know the content type upfront or read it
     * from headers. Till we fixed the REST layer to read the Content-Type header, that should be the only place where guessing is needed.
     */
    @Deprecated
    public static XContent xContent(byte[] data, int offset, int length) {
        XContentType type = xContentType(data, offset, length);
        if (type == null) {
            throw new XContentParseException("Failed to derive xcontent");
        }
        return xContent(type);
    }

    /**
     * Guesses the content type based on the provided input stream without consuming it.
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static XContentType xContentType(InputStream si) throws IOException {
        /*
         * We need to guess the content type. To do this, we look for the first non-whitespace character and then try to guess the content
         * type on the GUESS_HEADER_LENGTH bytes that follow. We do this in a way that does not modify the initial read position in the
         * underlying input stream. This is why the input stream must support mark/reset and why we repeatedly mark the read position and
         * reset.
         */
        if (si.markSupported() == false) {
            throw new IllegalArgumentException("Cannot guess the xcontent type without mark/reset support on " + si.getClass());
        }
        si.mark(Integer.MAX_VALUE);
        try {
            // scan until we find the first non-whitespace character or the end of the stream
            int current;
            do {
                current = si.read();
                if (current == -1) {
                    return null;
                }
            } while (Character.isWhitespace((char) current));
            // now guess the content type off the next GUESS_HEADER_LENGTH bytes including the current byte
            final byte[] firstBytes = new byte[GUESS_HEADER_LENGTH];
            firstBytes[0] = (byte) current;
            int read = 1;
            while (read < GUESS_HEADER_LENGTH) {
                final int r = si.read(firstBytes, read, GUESS_HEADER_LENGTH - read);
                if (r == -1) {
                    break;
                }
                read += r;
            }
            return xContentType(firstBytes, 0, read);
        } finally {
            si.reset();
        }

    }

    /**
     * Guesses the content type based on the provided bytes.
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static XContentType xContentType(byte[] bytes) {
        return xContentType(bytes, 0, bytes.length);
    }

    /**
     * Guesses the content type based on the provided bytes.
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static XContentType xContentType(byte[] bytes, int offset, int length) {
        int totalLength = bytes.length;
        if (totalLength == 0 || length == 0) {
            return null;
        } else if ((offset + length) > totalLength) {
            return null;
        }
        byte first = bytes[offset];
        if (JsonXContent.jsonXContent.detectContent(bytes, offset, length)) {
            return XContentType.JSON;
        }
        if (SmileXContent.smileXContent.detectContent(bytes, offset, length)) {
            return XContentType.SMILE;
        }
        if (YamlXContent.yamlXContent.detectContent(bytes, offset, length)) {
            return XContentType.YAML;
        }
        if (CborXContent.cborXContent.detectContent(bytes, offset, length)) {
            return XContentType.CBOR;
        }

        // fallback for JSON
        int jsonStart = 0;
        // JSON may be preceded by UTF-8 BOM
        if (length > 3 && first == (byte) 0xEF && bytes[offset + 1] == (byte) 0xBB && bytes[offset + 2] == (byte) 0xBF) {
            jsonStart = 3;
        }

        // a last chance for JSON
        for (int i = jsonStart; i < length; i++) {
            byte b = bytes[offset + i];
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
