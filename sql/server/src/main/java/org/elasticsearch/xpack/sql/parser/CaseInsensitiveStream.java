/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import java.util.Locale;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.IntStream;

// extension of ANTLR that does the uppercasing once for the whole stream
// the ugly part is that it has to duplicate LA method
class CaseInsensitiveStream extends ANTLRInputStream {
    // NOCOMMIT maybe we can fix this in the lexer or on the way in so we don't need the LA override
    protected char[] uppedChars;

    CaseInsensitiveStream(String input) {
        super(input);
        this.uppedChars = input.toUpperCase(Locale.ROOT).toCharArray();
    }

    // this part is copied from ANTLRInputStream
    @Override
    public int LA(int i) {
        if (i == 0) {
            return 0; // undefined
        }
        if (i < 0) {
            i++;
            if ((p + i - 1) < 0) {
                return IntStream.EOF;
            }
        }

        if ((p + i - 1) >= n) {
            return IntStream.EOF;
        }
        return uppedChars[p + i - 1];
    }
}
