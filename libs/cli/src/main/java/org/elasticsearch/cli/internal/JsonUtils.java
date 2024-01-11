/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

class JsonUtils {
    private JsonUtils() {}

    private static final char[] ESC_CODES = new char[128];
    static {
        ESC_CODES['\b'] = 'b';
        ESC_CODES['\t'] = 't';
        ESC_CODES['\n'] = 'n';
        ESC_CODES['\f'] = 'f';
        ESC_CODES['\r'] = 'r';
        ESC_CODES['"'] = '"';
        ESC_CODES['\\'] = '\\';
    }

    /**
     * Simple escape util for JSON.
     *
     * This doesn't do unicode encoding for any additional special, non-printable characters.
     */
    static void quoteAsString(CharSequence chars, StringBuilder sb) {
        if (chars == null) {
            sb.append("null");
            return;
        }
        int length = chars.length();
        int start = 0;

        for (int end = 0; end < length; end++) {
            char c = chars.charAt(end);
            // check for special chars in escape code table
            char ec = c < 128 ? ESC_CODES[c] : 0;
            if (ec != 0) {
                if (end > start) {
                    sb.append(chars, start, end);
                }
                sb.append('\\').append(ec); // append escaped
                start = end + 1;
            }
        }
        if (start < length) {
            sb.append(chars, start, length);
        }
    }
}
