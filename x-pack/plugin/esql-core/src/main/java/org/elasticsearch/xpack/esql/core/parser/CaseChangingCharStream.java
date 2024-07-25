/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;

// Wrapping stream for handling case-insensitive grammars

// This approach is taken from the ANTLR documentation
// https://github.com/antlr/antlr4/blob/master/doc/resources/CaseChangingCharStream.java
// https://github.com/antlr/antlr4/blob/master/doc/case-insensitive-lexing.md

/**
 * This class supports case-insensitive lexing by wrapping an existing
 * {@link CharStream} and forcing the lexer to see lowercase characters
 * Grammar literals should then be lower case such as {@code begin}.
 * The text of the character stream is unaffected.
 * <p>Example: input {@code BeGiN} would match lexer rule {@code begin}
 * but {@link CharStream#getText} will return {@code BeGiN}.
 * </p>
 */
public class CaseChangingCharStream implements CharStream {

    private final CharStream stream;

    /**
     * Constructs a new CaseChangingCharStream wrapping the given {@link CharStream} forcing
     * all characters to upper case or lower case.
     * @param stream The stream to wrap.
     */
    public CaseChangingCharStream(CharStream stream) {
        this.stream = stream;
    }

    @Override
    public String getText(Interval interval) {
        return stream.getText(interval);
    }

    @Override
    public void consume() {
        stream.consume();
    }

    @Override
    public int LA(int i) {
        int c = stream.LA(i);
        if (c <= 0) {
            return c;
        }
        return Character.toLowerCase(c);
    }

    @Override
    public int mark() {
        return stream.mark();
    }

    @Override
    public void release(int marker) {
        stream.release(marker);
    }

    @Override
    public int index() {
        return stream.index();
    }

    @Override
    public void seek(int index) {
        stream.seek(index);
    }

    @Override
    public int size() {
        return stream.size();
    }

    @Override
    public String getSourceName() {
        return stream.getSourceName();
    }
}
