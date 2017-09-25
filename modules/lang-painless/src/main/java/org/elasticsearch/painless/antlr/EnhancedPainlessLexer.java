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

package org.elasticsearch.painless.antlr;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.LexerNoViableAltException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.Pair;
import org.elasticsearch.painless.Definition;
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
    private final Definition definition;

    private Token stashedNext = null;
    private Token previous = null;

    EnhancedPainlessLexer(CharStream charStream, String sourceName, Definition definition) {
        super(charStream);
        this.sourceName = sourceName;
        this.definition = definition;
    }

    public Token getPreviousToken() {
        return previous;
    }

    @Override
    public Token nextToken() {
        if (stashedNext != null) {
            previous = stashedNext;
            stashedNext = null;
            return previous;
        }
        Token next = super.nextToken();
        if (insertSemicolon(previous, next)) {
            stashedNext = next;
            previous = _factory.create(new Pair<TokenSource, CharStream>(this, _input), PainlessLexer.SEMICOLON, ";",
                    Lexer.DEFAULT_TOKEN_CHANNEL, next.getStartIndex(), next.getStopIndex(), next.getLine(), next.getCharPositionInLine());
            return previous;
        } else {
            previous = next;
            return next;
        }
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
    protected boolean isSimpleType(String name) {
        return definition.isSimpleType(name);
    }

    @Override
    protected boolean slashIsRegex() {
        Token lastToken = getPreviousToken();
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

    private static boolean insertSemicolon(Token previous, Token next) {
        if (previous == null || next.getType() != PainlessLexer.RBRACK) {
            return false;
        }
        switch (previous.getType()) {
        case PainlessLexer.RBRACK:     // };} would be weird!
        case PainlessLexer.SEMICOLON:  // already have a semicolon, no need to add one
        case PainlessLexer.LBRACK:     // empty blocks don't need a semicolon
            return false;
        default:
            return true;
        }
    }
}
