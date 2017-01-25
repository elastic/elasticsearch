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
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenFactory;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.misc.Pair;

/**
 * Token factory that preserves that last non-whitespace token so you can do token level lookbehind in the lexer.
 */
public class StashingTokenFactory<T extends Token> implements TokenFactory<T> {
    private final TokenFactory<T> delegate;

    private T lastToken;

    public StashingTokenFactory(TokenFactory<T> delegate) {
        this.delegate = delegate;
    }

    public T getLastToken() {
        return lastToken;
    }

    @Override
    public T create(Pair<TokenSource, CharStream> source, int type, String text, int channel, int start, int stop, int line,
            int charPositionInLine) {
        return maybeStash(delegate.create(source, type, text, channel, start, stop, line, charPositionInLine));
    }

    @Override
    public T create(int type, String text) {
        return maybeStash(delegate.create(type, text));
    }

    private T maybeStash(T token) {
        if (token.getChannel() == Lexer.DEFAULT_TOKEN_CHANNEL) {
            lastToken = token;
        }
        return token;
    }
}
