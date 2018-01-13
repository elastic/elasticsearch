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

package org.elasticsearch.common.util;


import java.util.Arrays;
import java.util.Locale;
import java.util.MissingResourceException;

/**
 * Utilities for for dealing with {@link Locale} objects
 */
public class LocaleUtils {

    /**
     * Parse the given locale as {@code language}, {@code language-country} or
     * {@code language-country-variant}.
     * Either underscores or hyphens may be used as separators, but consistently, ie.
     * you may not use an hyphen to separate the language from the country and an
     * underscore to separate the country from the variant.
     * @throws IllegalArgumentException if there are too many parts in the locale string
     * @throws IllegalArgumentException if the language or country is not recognized
     */
    public static Locale parse(String localeStr) {
        boolean useUnderscoreAsSeparator = false;
        for (int i = 0; i < localeStr.length(); ++i) {
            final char c = localeStr.charAt(i);
            if (c == '-') {
                // the locale uses - as a separator, as expected
                break;
            } else if (c == '_') {
                useUnderscoreAsSeparator = true;
                break;
            }
        }

        final String[] parts;
        if (useUnderscoreAsSeparator) {
            parts = localeStr.split("_", -1);
        } else {
            parts = localeStr.split("-", -1);
        }

        final Locale locale = parseParts(parts);

        try {
            locale.getISO3Language();
        } catch (MissingResourceException e) {
            throw new IllegalArgumentException("Unknown language: " + parts[0], e);
        }

        try {
            locale.getISO3Country();
        } catch (MissingResourceException e) {
            throw new IllegalArgumentException("Unknown country: " + parts[1], e);
        }

        return locale;
    }

    /**
     * Parse the string describing a locale into a {@link Locale} object
     * for 5.x indices.
     */
    @Deprecated
    public static Locale parse5x(String localeStr) {
        final String[] parts = localeStr.split("_", -1);
        return parseParts(parts);
    }

    private static Locale parseParts(String[] parts) {
        switch (parts.length) {
        case 3:
            // lang, country, variant
            return new Locale(parts[0], parts[1], parts[2]);
        case 2:
            // lang, country
            return new Locale(parts[0], parts[1]);
        case 1:
            if ("ROOT".equalsIgnoreCase(parts[0])) {
                return Locale.ROOT;
            }
            // lang
            return new Locale(parts[0]);
        default:
            throw new IllegalArgumentException("Locales can have at most 3 parts but got " + parts.length + ": " + Arrays.asList(parts));
        }
    }
}
