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

import java.util.HashMap;
import java.util.Map;

/**
 * A message format in elasticsearch. Supports the following features:
 *<pre>
 *
 * - `{}`   indicates a place holder where single values will be translated to a string ({@code null}
 *          will be translated to a {@code "null"} string). Arrays will be enclosed in brackets (`[` and `]`).
 *
 * - `[]`   indicates a place holder where single values will be translated to a string enclosed in
 *          brackets `[` and `]`. Arrays will also be enclosed in brackets (but not double ones)
 *
 * examples:
 *
 *      PATTERN                 VALUE           OUTPUT
 *
 *      "value {}"              2               "value 2"
 *      "value {}"              null            "value null"
 *      "value []"              "text"          "value [text]"
 *      "value []"              null            "value [null]"
 *      "values {}"             [1, 2]          "values [1, 2]"
 *      "values []"             [1, 2]          "values [1, 2]"
 * </pre>
 */
public class ESMessageFormat {

    enum Delim {

        CURLY_BRACKETS('{', '}') {
            @Override
            public void append(StringBuilder sb, Object value) {
                try {
                    String oAsString = value.toString();
                    sb.append(oAsString);
                } catch (Throwable t) {
                    sb.append("[FAILED toString()]");
                }
            }
        },
        BRACKETS('[', ']') {
            @Override
            public void append(StringBuilder sb, Object value) {
                try {
                    String oAsString = value.toString();
                    sb.append(start).append(oAsString).append(end);
                } catch (Throwable t) {
                    sb.append("[FAILED toString()]");
                }
            }
        };

        protected final char start;
        protected final char end;
        protected final String str;

        Delim(char start, char end) {
            this.start = start;
            this.end = end;
            this.str = new String(new char[] { start, end });
        }

        public abstract void append(StringBuilder sb, Object value);

        public int indexOf(String text, int from) {
            return text.indexOf(str, from);
        }
    }

    private static final char ESCAPE_CHAR = '\\';

    public static String format(final String messagePattern, final Object... argArray) {
        return formatWithPrefix(null, messagePattern, argArray);
    }

    public static String formatWithPrefix(final String prefix, final String messagePattern, final Object... argArray) {
        if (messagePattern == null) {
            return null;
        }

        if (argArray == null || argArray.length == 0) {
            if (prefix == null) {
                return messagePattern;
            } else {
                return prefix + messagePattern;
            }
        }

        int i = 0;
        int j;
        int k;
        final StringBuilder sbuf = new StringBuilder(messagePattern.length() + 50);

        if (prefix != null) {
            sbuf.append(prefix);
        }

        for (int L = 0; L < argArray.length; L++) {

            Delim delim = Delim.CURLY_BRACKETS;

            j = Delim.CURLY_BRACKETS.indexOf(messagePattern, i);
            k = Delim.BRACKETS.indexOf(messagePattern, i);
            if (j < 0) {
                j = k;
                delim = Delim.BRACKETS;
            } else if (k > -1) {
                j = Math.min(j, k);
                delim = j == k ? Delim.BRACKETS : Delim.CURLY_BRACKETS;
            }

            if (j == -1) {
                // no more variables
                if (i == 0) { // this is a simple string
                    return messagePattern;
                } else { // add the tail string which contains no variables and return
                    // the result.
                    sbuf.append(messagePattern.substring(i, messagePattern.length()));
                    return sbuf.toString();
                }
            } else {
                if (isEscapedDelimiter(messagePattern, j)) {
                    if (!isDoubleEscaped(messagePattern, j)) {
                        L--; // Delim.start was escaped, thus should not be incremented
                        sbuf.append(messagePattern.substring(i, j - 1));
                        sbuf.append(delim.start);
                        i = j + 1;
                    } else {
                        // The escape character preceding the delimiter start is
                        // itself escaped: "abc x:\\{}"
                        // we have to consume one backward slash
                        sbuf.append(messagePattern.substring(i, j - 1));
                        deeplyAppendParameter(delim, sbuf, argArray[L], new HashMap());
                        i = j + 2;
                    }
                } else {
                    // normal case
                    sbuf.append(messagePattern.substring(i, j));
                    deeplyAppendParameter(delim, sbuf, argArray[L], new HashMap());
                    i = j + 2;
                }
            }
        }
        // append the characters following the last {} pair.
        sbuf.append(messagePattern.substring(i, messagePattern.length()));
        return sbuf.toString();
    }

    static boolean isEscapedDelimiter(String messagePattern, int delimiterStartIndex) {

        if (delimiterStartIndex == 0) {
            return false;
        }
        char potentialEscape = messagePattern.charAt(delimiterStartIndex - 1);
        if (potentialEscape == ESCAPE_CHAR) {
            return true;
        } else {
            return false;
        }
    }

    static boolean isDoubleEscaped(String messagePattern, int delimiterStartIndex) {
        if (delimiterStartIndex >= 2 && messagePattern.charAt(delimiterStartIndex - 2) == ESCAPE_CHAR) {
            return true;
        } else {
            return false;
        }
    }

    static void deeplyAppendParameter(Delim delim, StringBuilder sbuf, Object o, Map seenMap) {
        if (o == null) {
            delim.append(sbuf, "null");
            return;
        }
        if (!o.getClass().isArray()) {
            delim.append(sbuf, o);
        } else {
            // check for primitive array types because they
            // unfortunately cannot be cast to Object[]
            if (o instanceof boolean[]) {
                booleanArrayAppend(sbuf, (boolean[]) o);
            } else if (o instanceof byte[]) {
                byteArrayAppend(sbuf, (byte[]) o);
            } else if (o instanceof char[]) {
                charArrayAppend(sbuf, (char[]) o);
            } else if (o instanceof short[]) {
                shortArrayAppend(sbuf, (short[]) o);
            } else if (o instanceof int[]) {
                intArrayAppend(sbuf, (int[]) o);
            } else if (o instanceof long[]) {
                longArrayAppend(sbuf, (long[]) o);
            } else if (o instanceof float[]) {
                floatArrayAppend(sbuf, (float[]) o);
            } else if (o instanceof double[]) {
                doubleArrayAppend(sbuf, (double[]) o);
            } else {
                objectArrayAppend(sbuf, (Object[]) o, seenMap);
            }
        }
    }

    static void objectArrayAppend(StringBuilder sbuf, Object[] a, Map seenMap) {
        sbuf.append(Delim.BRACKETS.start);
        if (!seenMap.containsKey(a)) {
            seenMap.put(a, null);
            final int len = a.length;
            for (int i = 0; i < len; i++) {
                deeplyAppendParameter(Delim.CURLY_BRACKETS, sbuf, a[i], seenMap);
                if (i != len - 1)
                    sbuf.append(", ");
            }
            // allow repeats in siblings
            seenMap.remove(a);
        } else {
            sbuf.append("...");
        }
        sbuf.append(Delim.BRACKETS.end);
    }

    static void booleanArrayAppend(StringBuilder sbuf, boolean[] a) {
        sbuf.append(Delim.BRACKETS.start);
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1)
                sbuf.append(", ");
        }
        sbuf.append(Delim.BRACKETS.end);
    }

    static void byteArrayAppend(StringBuilder sbuf, byte[] a) {
        sbuf.append(Delim.BRACKETS.start);
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1)
                sbuf.append(", ");
        }
        sbuf.append(Delim.BRACKETS.end);
    }

    static void charArrayAppend(StringBuilder sbuf, char[] a) {
        sbuf.append(Delim.BRACKETS.start);
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1)
                sbuf.append(", ");
        }
        sbuf.append(Delim.BRACKETS.end);
    }

    static void shortArrayAppend(StringBuilder sbuf, short[] a) {
        sbuf.append(Delim.BRACKETS.start);
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1)
                sbuf.append(", ");
        }
        sbuf.append(Delim.BRACKETS.end);
    }

    static void intArrayAppend(StringBuilder sbuf, int[] a) {
        sbuf.append(Delim.BRACKETS.start);
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1)
                sbuf.append(", ");
        }
        sbuf.append(Delim.BRACKETS.end);
    }

    static void longArrayAppend(StringBuilder sbuf, long[] a) {
        sbuf.append(Delim.BRACKETS.start);
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1)
                sbuf.append(", ");
        }
        sbuf.append(Delim.BRACKETS.end);
    }

    static void floatArrayAppend(StringBuilder sbuf, float[] a) {
        sbuf.append(Delim.BRACKETS.start);
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1)
                sbuf.append(", ");
        }
        sbuf.append(Delim.BRACKETS.end);
    }

    static void doubleArrayAppend(StringBuilder sbuf, double[] a) {
        sbuf.append(Delim.BRACKETS.start);
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1)
                sbuf.append(", ");
        }
        sbuf.append(Delim.BRACKETS.end);
    }
}
