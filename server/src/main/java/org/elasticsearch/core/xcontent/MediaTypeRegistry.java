/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.core.xcontent;


import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.xcontent.spi.MediaTypeProvider;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parses supported internet media types
 *
 * @opensearch.internal
 */
public final class MediaTypeRegistry {
    private static Map<String, MediaType> formatToMediaType = Map.of();
    private static Map<String, MediaType> typeWithSubtypeToMediaType = Map.of();

    // Default mediaType singleton
    private static MediaType DEFAULT_MEDIA_TYPE;
    public static final int GUESS_HEADER_LENGTH = 20;

    // JSON is a core type, so we create a static instance for implementations that require JSON format (e.g., tests)
    // todo we should explore moving the concrete JSON implementation from the xcontent library to core
    public static final MediaType JSON;

    static {
        List<MediaType> mediaTypes = new ArrayList<>();
        Map<String, MediaType> amt = new HashMap<>();
        for (MediaTypeProvider provider : ServiceLoader.load(MediaTypeProvider.class, MediaTypeProvider.class.getClassLoader())) {
            mediaTypes.addAll(provider.getMediaTypes());
            amt = Stream.of(amt, provider.getAdditionalMediaTypes())
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        register(mediaTypes.toArray(new MediaType[0]), amt);
        JSON = fromMediaType("application/json");
        setDefaultMediaType(JSON);
    }

    private static void register(MediaType[] acceptedMediaTypes, Map<String, MediaType> additionalMediaTypes) {
        // ensures the map is not overwritten:
        Map<String, MediaType> typeMap = new HashMap<>(typeWithSubtypeToMediaType);
        Map<String, MediaType> formatMap = new HashMap<>(formatToMediaType);
        for (MediaType mediaType : acceptedMediaTypes) {
            if (formatMap.containsKey(mediaType.format())) {
                throw new IllegalArgumentException("unable to register mediaType: [" + mediaType.format() + "]. Type already exists.");
            }
            typeMap.put(mediaType.typeWithSubtype(), mediaType);
            formatMap.put(mediaType.format(), mediaType);
        }
        for (Map.Entry<String, MediaType> entry : additionalMediaTypes.entrySet()) {
            String typeWithSubtype = entry.getKey().toLowerCase(Locale.ROOT);
            if (typeMap.containsKey(typeWithSubtype)) {
                throw new IllegalArgumentException(
                    "unable to register mediaType: ["
                        + entry.getKey()
                        + "]. "
                        + "Type already exists and is mapped to: [."
                        + entry.getValue().format()
                        + "]"
                );
            }

            MediaType mediaType = entry.getValue();
            typeMap.put(typeWithSubtype, mediaType);
            formatMap.putIfAbsent(mediaType.format(), mediaType); // ignore if the additional type mapping already exists
        }

        formatToMediaType = Map.copyOf(formatMap);
        typeWithSubtypeToMediaType = Map.copyOf(typeMap);
    }

    public static MediaType fromMediaType(String mediaType) {
        ParsedMediaType parsedMediaType = parseMediaType(mediaType);
        return parsedMediaType != null ? parsedMediaType.getMediaType() : null;
    }

    public static MediaType fromFormat(String format) {
        if (format == null) {
            return null;
        }
        return formatToMediaType.get(format.toLowerCase(Locale.ROOT));
    }

    /**
     * Returns a binary content builder for the provided content type.
     */
    public static XContentBuilder contentBuilder(MediaType type) throws IOException {
        for (var mediaType : formatToMediaType.values()) {
            if (type == mediaType) {
                return type.contentBuilder();
            }
        }
        throw new IllegalArgumentException("No matching content type for " + type);
    }

    public static XContentBuilder contentBuilder(MediaType type, OutputStream outputStream) throws IOException {
        for (var mediaType : formatToMediaType.values()) {
            if (type == mediaType) {
                return type.contentBuilder(outputStream);
            }
        }
        throw new IllegalArgumentException("No matching content type for " + type);
    }

    /**
     * Guesses the content (type) based on the provided char sequence and returns the corresponding {@link XContent}
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static MediaType xContent(final byte[] data, int offset, int length) {
        MediaType type = mediaTypeFromBytes(data, offset, length);
        if (type == null) {
            throw new XContentParseException("Failed to derive xcontent");
        }
        return type;
    }

    /**
     * Guesses the content type based on the provided bytes and returns the corresponding {@link XContent}
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static MediaType xContent(byte[] data) {
        return xContent(data, 0, data.length);
    }

    /**
     * Guesses the content (type) based on the provided char sequence and returns the corresponding {@link XContent}
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static MediaType xContent(CharSequence content) {
        MediaType type = xContentType(content);
        if (type == null) {
            throw new XContentParseException("Failed to derive xcontent");
        }
        return type;
    }

    /**
     * Guesses the content type based on the provided char sequence.
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static MediaType xContentType(CharSequence content) {
        int length = content.length() < GUESS_HEADER_LENGTH ? content.length() : GUESS_HEADER_LENGTH;
        if (length == 0) {
            return null;
        }
        for (var mediaType : formatToMediaType.values()) {
            if (mediaType.detectedXContent(content, length)) {
                return mediaType;
            }
        }

        // fallback for json
        for (int i = 0; i < length; i++) {
            char c = content.charAt(i);
            if (c == '{') {
                return MediaType.fromMediaType("application/json");
            }
            if (Character.isWhitespace(c) == false) {
                break;
            }
        }
        return null;
    }

    /**
     * Guesses the content type based on the provided input stream without consuming it.
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static MediaType xContentType(InputStream si) throws IOException {
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
            return mediaTypeFromBytes(firstBytes, 0, read);
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
    public static MediaType xContentType(BytesReference bytes) {
        if (bytes instanceof BytesArray) {
            final BytesArray array = (BytesArray) bytes;
            return mediaTypeFromBytes(array.array(), array.offset(), array.length());
        }
        try {
            final InputStream inputStream = bytes.streamInput();
            assert inputStream.markSupported();
            return xContentType(inputStream);
        } catch (IOException e) {
            assert false : "Should not happen, we're just reading bytes from memory";
            throw new UncheckedIOException(e);
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
    public static MediaType mediaTypeFromBytes(final byte[] data, int offset, int length) {
        int totalLength = data.length;
        if (totalLength == 0 || length == 0) {
            return null;
        } else if ((offset + length) > totalLength) {
            return null;
        }
        for (var mediaType : formatToMediaType.values()) {
            if (mediaType.detectedXContent(data, offset, length)) {
                return mediaType;
            }
        }

        // a last chance for JSON
        int jsonStart = 0;
        // JSON may be preceded by UTF-8 BOM
        if (length > 3 && data[offset] == (byte) 0xEF && data[offset + 1] == (byte) 0xBB && data[offset + 2] == (byte) 0xBF) {
            jsonStart = 3;
        }

        for (int i = jsonStart; i < length; i++) {
            byte b = data[offset + i];
            if (b == '{') {
                return fromMediaType("application/json");
            }
            if (Character.isWhitespace(b) == false) {
                break;
            }
        }

        return null;
    }

    /**
     * parsing media type that follows https://tools.ietf.org/html/rfc7231#section-3.1.1.1
     * @param headerValue a header value from Accept or Content-Type
     * @return a parsed media-type
     */
    public static ParsedMediaType parseMediaType(String headerValue) {
        if (headerValue != null) {
            String[] split = headerValue.toLowerCase(Locale.ROOT).split(";");

            String[] typeSubtype = split[0].trim().split("/");
            if (typeSubtype.length == 2) {
                String type = typeSubtype[0];
                String subtype = typeSubtype[1];
                MediaType mediaType = typeWithSubtypeToMediaType.get(type + "/" + subtype);
                if (mediaType != null) {
                    Map<String, String> parameters = new HashMap<>();
                    for (int i = 1; i < split.length; i++) {
                        // spaces are allowed between parameters, but not between '=' sign
                        String[] keyValueParam = split[i].trim().split("=");
                        if (keyValueParam.length != 2 || hasSpaces(keyValueParam[0]) || hasSpaces(keyValueParam[1])) {
                            return null;
                        }
                        parameters.put(keyValueParam[0], keyValueParam[1]);
                    }
                    return new ParsedMediaType(mediaType, parameters);
                }
            }

        }
        return null;
    }

    private static boolean hasSpaces(String s) {
        return s.trim().equals(s) == false;
    }

    /**
     * A media type object that contains all the information provided on a Content-Type or Accept header
     */
    public static class ParsedMediaType {
        private final Map<String, String> parameters;
        private final MediaType mediaType;

        public ParsedMediaType(MediaType mediaType, Map<String, String> parameters) {
            this.parameters = parameters;
            this.mediaType = mediaType;
        }

        public MediaType getMediaType() {
            return mediaType;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }
    }

    private static void setDefaultMediaType(final MediaType mediaType) {
        if (DEFAULT_MEDIA_TYPE != null) {
            throw new RuntimeException(
                "unable to reset the default media type from current default [" + DEFAULT_MEDIA_TYPE + "] to [" + mediaType + "]"
            );
        } else {
            DEFAULT_MEDIA_TYPE = mediaType;
        }
    }

    public static MediaType getDefaultMediaType() {
        return DEFAULT_MEDIA_TYPE;
    }
}
