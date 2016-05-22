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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DiagnosticErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.elasticsearch.painless.antlr.PainlessParser.SourceBlockContext;
import org.elasticsearch.painless.ScriptTestCase;

public class ParserTests extends ScriptTestCase {
    private SourceBlockContext buildAntlrTree(String source) {
        ANTLRInputStream stream = new ANTLRInputStream(source);
        PainlessLexer lexer = new ErrorHandlingLexer(stream);
        PainlessParser parser = new PainlessParser(new CommonTokenStream(lexer));
        ParserErrorStrategy strategy = new ParserErrorStrategy();

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        // Diagnostic listener invokes syntaxError on other listeners for ambiguity issues,
        parser.addErrorListener(new DiagnosticErrorListener(true));
        // a second listener to fail the test when the above happens.
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(final Recognizer<?,?> recognizer, final Object offendingSymbol, final int line,
                                    final int charPositionInLine, final String msg, final RecognitionException e) {
                throw new AssertionError("line: " + line + ", offset: " + charPositionInLine +
                    ", symbol:" + offendingSymbol + " " + msg);
            }
        });

        // Enable exact ambiguity detection (costly). we enable exact since its the default for
        // DiagnosticErrorListener, life is too short to think about what 'inexact ambiguity' might mean.
        parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
        parser.setErrorHandler(strategy);

        return parser.sourceBlock();
    }

    public void testIllegalSecondary() {
        buildAntlrTree("((2+2)((Map.Entry)x).y).z");
    }
}
