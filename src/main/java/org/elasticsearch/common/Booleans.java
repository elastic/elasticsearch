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

    public static boolean parseBoolean(char[] text, int offset, int length, boolean defaultValue) {
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


    public static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return !(value.equals("false") || value.equals("0") || value.equals("off") || value.equals("no"));
    }

    public static Boolean parseBoolean(String value, Boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return !(value.equals("false") || value.equals("0") || value.equals("off") || value.equals("no"));
    }

    public static boolean isExplicitFalse(String value) {
        return (value.equals("false") || value.equals("0") || value.equals("off") || value.equals("no"));
    }

    public static boolean isExplicitTrue(String value) {
        return (value.equals("true") || value.equals("1") || value.equals("on") || value.equals("yes"));
    }

}
