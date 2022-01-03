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
import org.elasticsearch.painless.lookup.PainlessLookup;

/**
 * A lexer that is customized for painless suggestions with the following modifications:
 * <ul>
 * <li>Overrides the default error behavior to only fail if we don't recognize a token in default mode
 * <li>Stores the last token in case we need to do lookbehind for regex vs division detection
 * <li>Implements the regex vs division detection
 * <li>Enhances the error message when a string contains invalid escape sequences to include a list of valid escape sequences
 * </ul>
 */
public final class EnhancedSuggestLexer extends SuggestLexer {

    private Token current = null;
    private final PainlessLookup painlessLookup;

    public EnhancedSuggestLexer(CharStream charStream, PainlessLookup painlessLookup) {
        super(charStream);
        this.painlessLookup = painlessLookup;
    }

    @Override
    public Token nextToken() {
        current = super.nextToken();
        return current;
    }

    @Override
    public void recover(final LexerNoViableAltException lnvae) {
        if (this._mode != PainlessLexer.DEFAULT_MODE) {
            this._mode = DEFAULT_MODE;
        } else {
            throw new IllegalStateException("unexpected token [" + lnvae.getOffendingToken().getText() + "]", lnvae);
        }
    }

    @Override
    protected boolean isSlashRegex() {
        Token lastToken = current;
        if (lastToken == null) {
            return true;
        }
        return switch (lastToken.getType()) {
            // tag::noformat
            case PainlessLexer.RBRACE, PainlessLexer.RP, PainlessLexer.OCTAL, PainlessLexer.HEX, PainlessLexer.INTEGER,
                 PainlessLexer.DECIMAL, PainlessLexer.ID, PainlessLexer.DOTINTEGER, PainlessLexer.DOTID -> false;
            // end::noformat
            default -> true;
        };
    }

    @Override
    protected boolean isType(String text) {
        return painlessLookup.isValidCanonicalClassName(text);
    }
}
