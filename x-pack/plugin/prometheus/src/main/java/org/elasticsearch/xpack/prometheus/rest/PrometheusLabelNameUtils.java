/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

/**
 * Utilities for working with Prometheus label names, including decoding of the
 * {@code U__} encoding defined by the OpenMetrics spec to represent characters
 * that are not valid in Prometheus label names (e.g. dots, colons).
 */
final class PrometheusLabelNameUtils {

    private PrometheusLabelNameUtils() {}

    /**
     * Decodes a label name that may use the {@code U__} encoding.
     *
     * <p>If the name does not start with {@code "U__"} it is returned as-is.
     * Otherwise the {@code "U__"} prefix is stripped and the rest is decoded
     * left-to-right:
     * <ul>
     *   <li>{@code "__"} → {@code '_'}</li>
     *   <li>{@code "_HEX_"} where HEX is a hex codepoint → the corresponding Unicode character</li>
     *   <li>any other character → passed through unchanged</li>
     * </ul>
     */
    static String decodeLabelName(String name) {
        if (name == null || name.startsWith("U__") == false) {
            return name;
        }
        String encoded = name.substring(3); // strip "U__"
        StringBuilder sb = new StringBuilder(encoded.length());
        int i = 0;
        while (i < encoded.length()) {
            if (encoded.startsWith("__", i)) {
                sb.append('_');
                i += 2;
            } else if (encoded.charAt(i) == '_') {
                // Possibly a hex escape: _HEX_ where HEX is one or more hex digits
                int closeIdx = encoded.indexOf('_', i + 1);
                if (closeIdx > i + 1) {
                    String hexPart = encoded.substring(i + 1, closeIdx);
                    if (hexPart.isEmpty() == false && isHex(hexPart)) {
                        try {
                            int codePoint = Integer.parseInt(hexPart, 16);
                            sb.appendCodePoint(codePoint);
                            i = closeIdx + 1;
                            continue;
                        } catch (IllegalArgumentException ignored) {
                            // fall through to pass-through
                        }
                    }
                }
                // Graceful degradation: not a valid hex escape, pass through
                sb.append(encoded.charAt(i));
                i++;
            } else {
                sb.append(encoded.charAt(i));
                i++;
            }
        }
        return sb.toString();
    }

    private static boolean isHex(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
                continue;
            }
            return false;
        }
        return true;
    }
}
