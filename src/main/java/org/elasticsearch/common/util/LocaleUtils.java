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


import java.util.Locale;

/**
 * Utilities for for dealing with {@link Locale} objects
 */
public class LocaleUtils {

    /**
     * Parse the string describing a locale into a {@link Locale} object
     */
    public static Locale parse(String localeStr) {
        final String[] parts = localeStr.split("_", -1);
        switch (parts.length) {
            case 3:
                // lang_country_variant
                return new Locale(parts[0], parts[1], parts[2]);
            case 2:
                // lang_country
                return new Locale(parts[0], parts[1]);
            case 1:
                if ("ROOT".equalsIgnoreCase(parts[0])) {
                    return Locale.ROOT;
                }
                // lang
                return new Locale(parts[0]);
            default:
                throw new IllegalArgumentException("Can't parse locale: [" + localeStr + "]");
        }
    }

    /**
     * Return a string for a {@link Locale} object
     */
    public static String toString(Locale locale) {
        // JAVA7 - use .toLanguageTag instead of .toString()
        return locale.toString();
    }
}
