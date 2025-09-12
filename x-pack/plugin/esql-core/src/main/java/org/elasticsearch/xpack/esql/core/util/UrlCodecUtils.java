/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.PercentCodec;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class UrlCodecUtils {

    private UrlCodecUtils() {}

    private static final PercentCodec urlEncodeCodec;
    private static final PercentCodec urlEncodeComponentCodec;

    static {
        // both codecs percent-encode all characters in the input except for alphanumerics, '-', '.', '_', and '~'.

        // encodes spaces as '+'
        byte[] b1 = buildUnsafeBytes(Set.of(' '));
        urlEncodeCodec = new PercentCodec(b1, true);

        // encodes spaces as '%20'
        byte[] b2 = buildUnsafeBytes(Collections.emptySet());
        urlEncodeComponentCodec = new PercentCodec(b2, false);
    }

    public static byte[] encodeUrl(byte[] bytes) {
        byte[] encoded = null;

        try {
            encoded = urlEncodeCodec.encode(bytes);
        } catch (EncoderException ex) {
            throw new RuntimeException(ex);
        }

        return encoded;
    }

    public static byte[] encodeUrlComponent(byte[] bytes) {
        byte[] encoded = null;

        try {
            encoded = urlEncodeComponentCodec.encode(bytes);
        } catch (EncoderException ex) {
            throw new RuntimeException(ex);
        }

        return encoded;
    }

    /**
     * Builds the list of individual ASCII bytes that are considered unsafe; must always be percent-encoded. Bytes outside the
     * ASCII range are always percent-encoded by the codecs are don't need to be included in our list.
     *
     * @param additionallySafe
     * @return unsafe ASCII chars
     */
    private static byte[] buildUnsafeBytes(final Set<Character> additionallySafe) {
        Set<Byte> unsafe = new HashSet<>();

        for (int i = 0; i <= Byte.MAX_VALUE; ++i) {
            char c = (char) i;
            if (additionallySafe.contains(c) == false && isRfc3986Safe(c) == false) {
                unsafe.add((byte) i);
            }
        }

        byte[] bytes = new byte[unsafe.size()];

        int i = 0;
        for (byte b : unsafe) {
            bytes[i++] = b;
        }

        return bytes;
    }

    private static boolean isRfc3986Safe(char c) {
        // unreserved (aka: safe) chars according to RFC3986 remain unchanged after encoding
        return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~';
    }

}
