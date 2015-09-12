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

/**
 *
 */
public class Booleans {

    /**
     * Returns <code>false</code> if text is in <tt>false</tt>, <tt>0</tt>, <tt>off</tt>, <tt>no</tt>; else, true
     */
    public static boolean parseBoolean(char[] text, int offset, int length, boolean defaultValue) {
        // TODO: the leniency here is very dangerous: a simple typo will be misinterpreted and the user won't know.
        // We should remove it and cutover to https://github.com/rmuir/booleanparser
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
            return !(text[offset] == 'f' && text[offset + 1] == 'a' && text[offset + 2] == 'l' && text[offset + 3] == 's' && text[offset + 4] == 'e');
        }
        return true;
    }

    /**
     * returns true if the a sequence of chars is one of "true","false","on","off","yes","no","0","1"
     *
     * @param text   sequence to check
     * @param offset offset to start
     * @param length length to check
     */
    public static boolean isBoolean(char[] text, int offset, int length) {
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
            return (text[offset] == 'f' && text[offset + 1] == 'a' && text[offset + 2] == 'l' && text[offset + 3] == 's' && text[offset + 4] == 'e');
        }
        return false;
    }

    /***
     *
     * @param value
     * @return true/false
     * throws exception if string cannot be parsed to boolean
     */
    public static Boolean parseBooleanExact(String value) {

        boolean isFalse = isExplicitFalse(value);
        if (isFalse) {
            return false;
        }
        boolean isTrue = isExplicitTrue(value);
        if (isTrue) {
            return true;
        }

        throw new IllegalArgumentException("value cannot be parsed to boolean [ true/1/on/yes OR false/0/off/no ]  ");
    }

    public static Boolean parseBoolean(String value, Boolean defaultValue) {
        if (value == null) { // only for the null case we do that here!
            return defaultValue;
        }
        return parseBoolean(value, false);
    }
    /**
     * Returns <code>true</code> iff the value is neither of the following:
     *   <tt>false</tt>, <tt>0</tt>, <tt>off</tt>, <tt>no</tt>
     *   otherwise <code>false</code>
     */
    public static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return !(value.equals("false") || value.equals("0") || value.equals("off") || value.equals("no"));
    }

    /**
     * Returns <code>true</code> iff the value is either of the following:
     *   <tt>false</tt>, <tt>0</tt>, <tt>off</tt>, <tt>no</tt>
     *   otherwise <code>false</code>
     */
    public static boolean isExplicitFalse(String value) {
        return value != null && (value.equals("false") || value.equals("0") || value.equals("off") || value.equals("no"));
    }

    /**
     * Returns <code>true</code> iff the value is either of the following:
     *   <tt>true</tt>, <tt>1</tt>, <tt>on</tt>, <tt>yes</tt>
     *   otherwise <code>false</code>
     */
    public static boolean isExplicitTrue(String value) {
        return value != null && (value.equals("true") || value.equals("1") || value.equals("on") || value.equals("yes"));
    }

}
