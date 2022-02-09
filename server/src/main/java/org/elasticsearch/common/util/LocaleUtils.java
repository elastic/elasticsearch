/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
                throw new IllegalArgumentException(
                    "Locales can have at most 3 parts but got " + parts.length + ": " + Arrays.asList(parts)
                );
        }
    }
}
