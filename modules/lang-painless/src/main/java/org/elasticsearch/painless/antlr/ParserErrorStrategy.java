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

import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.elasticsearch.painless.Location;

/**
 * An error strategy that will override the default error behavior to fail on the first parser error.
 */
final class ParserErrorStrategy extends DefaultErrorStrategy {
    final String sourceName;
    
    ParserErrorStrategy(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public void recover(final Parser recognizer, final RecognitionException re) {
        final Token token = re.getOffendingToken();
        String message;

        if (token == null) {
            message = "no parse token found.";
        } else if (re instanceof InputMismatchException) {
            message = "unexpected token [" + getTokenErrorDisplay(token) + "]" +
                    " was expecting one of [" + re.getExpectedTokens().toString(recognizer.getVocabulary()) + "].";
        } else if (re instanceof NoViableAltException) {
            if (token.getType() == PainlessParser.EOF) {
                message = "unexpected end of script.";
            } else {
                message = "invalid sequence of tokens near [" + getTokenErrorDisplay(token) + "].";
            }
        } else {
            message =  "unexpected token near [" + getTokenErrorDisplay(token) + "].";
        }

        Location location = new Location(sourceName, token == null ? -1 : token.getStartIndex());
        throw location.createError(new IllegalArgumentException(message, re));
    }

    @Override
    public Token recoverInline(final Parser recognizer) throws RecognitionException {
        final Token token = recognizer.getCurrentToken();
        final String message = "unexpected token [" + getTokenErrorDisplay(token) + "]" +
            " was expecting one of [" + recognizer.getExpectedTokens().toString(recognizer.getVocabulary()) + "].";

        Location location = new Location(sourceName, token.getStartIndex());
        throw location.createError(new IllegalArgumentException(message));
    }

    @Override
    public void sync(final Parser recognizer) {
    }
}
