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
import org.elasticsearch.painless.Location;

/**
 * A lexer that is customized for painless. It will:
 * <ul>
 * <li>will override the default error behavior to fail on the first error
 * <li>store the last token in case we need to do lookbehind for semicolon insertion and regex vs division detection
 * <li>insert semicolons where they'd improve the language's readability. Rather than hack this into the parser and create a ton of
 * ambiguity we hack them here where we can use heuristics to do it quickly.
 * </ul>
 */
final class EnhancedPainlessLexer extends PainlessLexer {
    final String sourceName;
    private Token stashedNext = null;
    private Token previous = null;

    EnhancedPainlessLexer(CharStream charStream, String sourceName) {
        super(charStream);
        this.sourceName = sourceName;
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
        throw location.createError(new IllegalArgumentException("unexpected character [" + getErrorDisplay(text) + "].", lnvae));
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
