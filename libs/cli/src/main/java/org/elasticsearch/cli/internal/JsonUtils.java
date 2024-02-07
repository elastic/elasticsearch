/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import java.nio.CharBuffer;

/**
 * Simple escape util for JSON.
 *
 * Note, this doesn't do unicode encoding for any additional special, non-printable characters.
 */
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

    static void quoteAsString(char[] chars, int start, int count, StringBuilder sb) {
        quoteAsString(CharBuffer.wrap(chars), start, count, sb);
    }

    static void quoteAsString(CharSequence chars, int start, int count, StringBuilder sb) {
        final int end = start + count;
        assert chars != null;
        assert end <= chars.length();

        for (int current = start; current < end; current++) {
            char c = chars.charAt(current);
            // check for special chars in escape code table
            char ec = c < 128 ? ESC_CODES[c] : 0;
            if (ec != 0) {
                if (current > start) {
                    sb.append(chars, start, current);
                }
                sb.append('\\').append(ec); // append escaped
                start = current + 1;
            }
        }
        if (start < end) {
            sb.append(chars, start, end);
        }
    }

    static void quote(char c, StringBuilder sb) {
        // check for special chars in escape code table
        char ec = c < 128 ? ESC_CODES[c] : 0;
        if (ec != 0) {
            sb.append('\\').append(ec); // append escaped
        } else {
            sb.append(c);
        }
    }
}
