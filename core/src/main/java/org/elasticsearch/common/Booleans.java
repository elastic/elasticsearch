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

package org.elasticsearch.common;

public final class Booleans {
    private Booleans() {
        throw new AssertionError("No instances intended");
    }

    /**
     * Parses a char[] representation of a boolean value to <code>boolean</code>.
     *
     * @return <code>true</code> iff the sequence of chars is "true", <code>false</code> iff the sequence of chars is "false" or the
     * provided default value iff either text is <code>null</code> or length == 0.
     * @throws IllegalArgumentException if the string cannot be parsed to boolean.
     */
    public static boolean parseBoolean(char[] text, int offset, int length, boolean defaultValue) {
        if (text == null || length == 0) {
            return defaultValue;
        } else {
            return parseBoolean(new String(text, offset, length));
        }
    }

    /**
     * returns true iff the sequence of chars is one of "true","false".
     *
     * @param text   sequence to check
     * @param offset offset to start
     * @param length length to check
     */
    public static boolean isBoolean(char[] text, int offset, int length) {
        if (text == null || length == 0) {
            return false;
        }
        return isBoolean(new String(text, offset, length));
    }

    public static boolean isBoolean(String value) {
        return isFalse(value) || isTrue(value);
    }

    /**
     * Parses a string representation of a boolean value to <code>boolean</code>.
     *
     * @return <code>true</code> iff the provided value is "true". <code>false</code> iff the provided value is "false".
     * @throws IllegalArgumentException if the string cannot be parsed to boolean.
     */
    public static boolean parseBoolean(String value) {
        if (isFalse(value)) {
            return false;
        }
        if (isTrue(value)) {
            return true;
        }
        throw new IllegalArgumentException("Failed to parse value [" + value + "] as only [true] or [false] are allowed.");
    }

    /**
     *
     * @param value text to parse.
     * @param defaultValue The default value to return if the provided value is <code>null</code>.
     * @return see {@link #parseBoolean(String)}
     */
    public static boolean parseBoolean(String value, boolean defaultValue) {
        if (Strings.hasText(value)) {
            return parseBoolean(value);
        }
        return defaultValue;
    }

    public static Boolean parseBoolean(String value, Boolean defaultValue) {
        if (Strings.hasText(value)) {
            return parseBoolean(value);
        }
        return defaultValue;
    }

    /**
     * Returns <code>false</code> if text is in <tt>false</tt>, <tt>0</tt>, <tt>off</tt>, <tt>no</tt>; else, true
     *
     * @deprecated Only kept to provide automatic upgrades for pre 6.0 indices. Use {@link #parseBoolean(String, Boolean)} instead.
     */
    @Deprecated
    public static Boolean parseBooleanLenient(String value, Boolean defaultValue) {
        if (value == null) { // only for the null case we do that here!
            return defaultValue;
        }
        return parseBooleanLenient(value, false);
    }
    /**
     * Returns <code>true</code> iff the value is neither of the following:
     *   <tt>false</tt>, <tt>0</tt>, <tt>off</tt>, <tt>no</tt>
     *   otherwise <code>false</code>
     *
     * @deprecated Only kept to provide automatic upgrades for pre 6.0 indices. Use {@link #parseBoolean(String, boolean)} instead.
     */
    @Deprecated
    public static boolean parseBooleanLenient(String value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return !(value.equals("false") || value.equals("0") || value.equals("off") || value.equals("no"));
    }

    /**
     * @return <code>true</code> iff the value is <tt>false</tt>, otherwise <code>false</code>.
     */
    public static boolean isFalse(String value) {
        return "false".equals(value);
    }

    /**
     * @return <code>true</code> iff the value is <tt>true</tt>, otherwise <code>false</code>
     */
    public static boolean isTrue(String value) {
        return "true".equals(value);
    }

    /**
     * Returns <code>false</code> if text is in <tt>false</tt>, <tt>0</tt>, <tt>off</tt>, <tt>no</tt>; else, true
     *
     * @deprecated Only kept to provide automatic upgrades for pre 6.0 indices. Use {@link #parseBoolean(char[], int, int, boolean)} instead
     */
    @Deprecated
    public static boolean parseBooleanLenient(char[] text, int offset, int length, boolean defaultValue) {
        if (text == null || length == 0) {
            return defaultValue;
        }
        if (length == 1) {
            return text[offset] != '0';
        }
        if (length == 2) {
            return !(text[offset] == 'n' && text[offset + 1] == 'o');
        }
        if (length == 3) {
            return !(text[offset] == 'o' && text[offset + 1] == 'f' && text[offset + 2] == 'f');
        }
        if (length == 5) {
            return !(text[offset] == 'f' && text[offset + 1] == 'a' && text[offset + 2] == 'l' && text[offset + 3] == 's' &&
                text[offset + 4] == 'e');
        }
        return true;
    }

    /**
     * returns true if the a sequence of chars is one of "true","false","on","off","yes","no","0","1"
     *
     * @param text   sequence to check
     * @param offset offset to start
     * @param length length to check
     *
     * @deprecated Only kept to provide automatic upgrades for pre 6.0 indices. Use {@link #isBoolean(char[], int, int)} instead.
     */
    @Deprecated
    public static boolean isBooleanLenient(char[] text, int offset, int length) {
        if (text == null || length == 0) {
            return false;
        }
        if (length == 1) {
            return text[offset] == '0' || text[offset] == '1';
        }
        if (length == 2) {
            return (text[offset] == 'n' && text[offset + 1] == 'o') || (text[offset] == 'o' && text[offset + 1] == 'n');
        }
        if (length == 3) {
            return (text[offset] == 'o' && text[offset + 1] == 'f' && text[offset + 2] == 'f') ||
                (text[offset] == 'y' && text[offset + 1] == 'e' && text[offset + 2] == 's');
        }
        if (length == 4) {
            return (text[offset] == 't' && text[offset + 1] == 'r' && text[offset + 2] == 'u' && text[offset + 3] == 'e');
        }
        if (length == 5) {
            return (text[offset] == 'f' && text[offset + 1] == 'a' && text[offset + 2] == 'l' && text[offset + 3] == 's' &&
                text[offset + 4] == 'e');
        }
        return false;
    }

}
