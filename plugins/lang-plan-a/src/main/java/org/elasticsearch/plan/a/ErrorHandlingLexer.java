package org.elasticsearch.plan.a;

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

import java.text.ParseException;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.LexerNoViableAltException;
import org.antlr.v4.runtime.misc.Interval;

class ErrorHandlingLexer extends PlanALexer {
    public ErrorHandlingLexer(CharStream charStream) {
        super(charStream);
    }

    @Override
    public void recover(LexerNoViableAltException lnvae) {
        CharStream charStream = lnvae.getInputStream();
        int startIndex = lnvae.getStartIndex();
        String text = charStream.getText(Interval.of(startIndex, charStream.index()));

        ParseException parseException = new ParseException("Error [" + _tokenStartLine + ":" +
                _tokenStartCharPositionInLine + "]: unexpected character [" +
                getErrorDisplay(text) + "].",  _tokenStartCharIndex);
        parseException.initCause(lnvae);
        throw new RuntimeException(parseException);
    }
}
