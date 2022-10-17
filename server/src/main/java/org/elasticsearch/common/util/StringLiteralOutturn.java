/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Setting;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Copy an intern String object to a non-intern String object (aka outturn).
 */
public final class StringLiteralOutturn {

    /**
     * Java Strings use UTF-16. In theory, copying UTF-16 to UTF-16 should be fast because it skips UTF-8 conversion.
     * In practice, UTF-8 is highly optimized, so it is faster than using UTF-16. See StringLiteralOutturnTests.
     */
    private static final Charset DEFAULT_INTERMEDIATE_CHARSET = StandardCharsets.UTF_8;

    /**
     * Copy intern String characters to a new non-intern String object.
     * @param internString String that is assumed to be interned.
     * @return New String object which is not interned.
     */
    public static String outturn(final String internString) {
        return outturn(internString, DEFAULT_INTERMEDIATE_CHARSET);
    }

    public static String outturn(final String internString, final Charset intermediateCharset) {
        final byte[] keyBytes = internString.getBytes(intermediateCharset); // copy bytes to a new, non-intern String object
        return new String(keyBytes, 0, keyBytes.length, intermediateCharset);
    }

    public static class MapStringKey<Value> {
        /**
         * Copy all entries to a new Map, but use new non-intern copies of the String keys.
         * For example, this can be used to convert an IdentityHashMap to a Map which complies with the equals(), get(), etc Map APIs.
         * @param map Map with String keys that are assumed to be interned.
         * @return New Map with non-intern String keys.
         */
        public static <Value> Map<String, Value> outturn(Map<String, Value> map) {
            final Map<String, Value> nonInternMap;
            nonInternMap = new HashMap<>();
            for (final Map.Entry<String, Value> entry : map.entrySet()) {
                nonInternMap.put(StringLiteralOutturn.outturn(entry.getKey()), entry.getValue());
            }
            return nonInternMap;
        }
    }

    public static class MapSimpleKey<Value> {
        /**
         * Copy all entries to a new Map, but use new non-intern copies of the String keys.
         * For example, this can be used to convert an IdentityHashMap to a Map which complies with the equals(), get(), etc Map APIs.
         * @param map Map with Setting.SimpleKey keys that are assumed to contain interned Strings.
         * @return New Map with Setting.SimpleKey keys containing non-intern Strings.
         */
        public static <Value> Map<Setting.SimpleKey, Value> outturn(Map<Setting.SimpleKey, Value> map) {
            final Map<Setting.SimpleKey, Value> nonInternMap;
            nonInternMap = new HashMap<>();
            for (final Map.Entry<Setting.SimpleKey, Value> entry : map.entrySet()) {
                nonInternMap.put(new Setting.SimpleKey(StringLiteralOutturn.outturn(entry.getKey().toString())), entry.getValue());
            }
            return nonInternMap;
        }
    }
}
