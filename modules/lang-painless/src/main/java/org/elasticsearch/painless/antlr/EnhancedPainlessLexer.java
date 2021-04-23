/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.antlr;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.LexerNoViableAltException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.elasticsearch.painless.Location;

/**
 * A lexer that is customized for painless. It:
 * <ul>
 * <li>Overrides the default error behavior to fail on the first error.
 * <li>Stores the last token in case we need to do lookbehind for semicolon insertion and regex vs division detection.
 * <li>Implements the regex vs division detection.
 * <li>Insert semicolons where they'd improve the language's readability. Rather than hack this into the parser and create a ton of
 * ambiguity we hack them here where we can use heuristics to do it quickly.
 * <li>Enhances the error message when a string contains invalid escape sequences to include a list of valid escape sequences.
 * </ul>
 */
final class EnhancedPainlessLexer extends PainlessLexer {
    private final String sourceName;
    private Token current = null;

    EnhancedPainlessLexer(CharStream charStream, String sourceName) {
        super(charStream);
        this.sourceName = sourceName;
    }

    @Override
    public Token nextToken() {
        current = super.nextToken();
        return current;
    }

    @Override
    public void recover(final LexerNoViableAltException lnvae) {
        final CharStream charStream = lnvae.getInputStream();
        final int startIndex = lnvae.getStartIndex();
        final String text = charStream.getText(Interval.of(startIndex, charStream.index()));

        Location location = new Location(sourceName, _tokenStartCharIndex);
        String message = "unexpected character [" + getErrorDisplay(text) + "].";
        char firstChar = text.charAt(0);
        if ((firstChar == '\'' || firstChar == '"') && text.length() - 2 > 0 && text.charAt(text.length() - 2) == '\\') {
            /* Use a simple heuristic to guess if the unrecognized characters were trying to be a string but has a broken escape sequence.
             * If it was add an extra message about valid string escape sequences. */
            message += " The only valid escape sequences in strings starting with [" + firstChar + "] are [\\\\] and [\\"
                    + firstChar + "].";
        }
        throw location.createError(new IllegalArgumentException(message, lnvae));
    }

    @Override
    protected boolean isSlashRegex() {
        Token lastToken = current;
        if (lastToken == null) {
            return true;
        }
        switch (lastToken.getType()) {
        case PainlessLexer.RBRACE:
        case PainlessLexer.RP:
        case PainlessLexer.OCTAL:
        case PainlessLexer.HEX:
        case PainlessLexer.INTEGER:
        case PainlessLexer.DECIMAL:
        case PainlessLexer.ID:
        case PainlessLexer.DOTINTEGER:
        case PainlessLexer.DOTID:
            return false;
        default:
            return true;
        }
    }
}
