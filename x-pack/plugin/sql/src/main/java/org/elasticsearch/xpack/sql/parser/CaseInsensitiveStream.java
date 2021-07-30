/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.IntStream;

import java.util.Locale;

// extension of ANTLR that does the upper-casing once for the whole stream
// the ugly part is that it has to duplicate LA method

// This approach is the official solution from the ANTLR authors
// in that it's both faster and easier than having a dedicated lexer
// see https://github.com/antlr/antlr4/issues/1002
class CaseInsensitiveStream extends ANTLRInputStream {
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
