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
import org.elasticsearch.painless.antlr.PainlessParser.SourceContext;
import org.elasticsearch.painless.ScriptTestCase;

import java.text.ParseException;

public class ParserTests extends ScriptTestCase {
    private static class TestException extends RuntimeException {
        TestException(String msg) {
            super(msg);
        }
    }

    private SourceContext buildAntlrTree(String source) {
        ANTLRInputStream stream = new ANTLRInputStream(source);
        PainlessLexer lexer = new EnhancedPainlessLexer(stream, "testing");
        PainlessParser parser = new PainlessParser(new CommonTokenStream(lexer));
        ParserErrorStrategy strategy = new ParserErrorStrategy("testing");

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        // Diagnostic listener invokes syntaxError on other listeners for ambiguity issues,
        parser.addErrorListener(new DiagnosticErrorListener(true));
        // a second listener to fail the test when the above happens.
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line,
                                    final int charPositionInLine, final String msg, final RecognitionException e) {
                throw new TestException("line: " + line + ", offset: " + charPositionInLine +
                    ", symbol:" + offendingSymbol + " " + msg);
            }
        });

        // Enable exact ambiguity detection (costly). we enable exact since its the default for
        // DiagnosticErrorListener, life is too short to think about what 'inexact ambiguity' might mean.
        parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
        parser.setErrorHandler(strategy);

        return parser.source();
    }

    public void testIllegalSecondary() {
        //TODO: Need way more corner case tests.
        Exception exception = expectThrows(TestException.class, () -> buildAntlrTree("(x = 5).y"));
        assertTrue(exception.getMessage().contains("no viable alternative"));
        exception = expectThrows(TestException.class, () -> buildAntlrTree("((x = 5).y = 2).z;"));
        assertTrue(exception.getMessage().contains("no viable alternative"));
        exception = expectThrows(TestException.class, () -> buildAntlrTree("(2 + 2).z"));
        assertTrue(exception.getMessage().contains("no viable alternative"));
        exception = expectThrows(RuntimeException.class, () -> buildAntlrTree("((Map)x.-x)"));
        assertTrue(exception.getMessage().contains("unexpected character"));
    }

    public void testLambdaSyntax() {
        buildAntlrTree("call(p -> {p.doSomething();});");
        buildAntlrTree("call(int p -> {p.doSomething();});");
        buildAntlrTree("call((p, u, v) -> {p.doSomething(); blah = 1;});");
        buildAntlrTree("call(1, (p, u, v) -> {p.doSomething(); blah = 1;}, 3);");
        buildAntlrTree("call((p, u, v) -> {p.doSomething(); blah = 1;});");
        buildAntlrTree("call(x, y, z, (int p, int u, int v) -> {p.doSomething(); blah = 1;});");
        buildAntlrTree("call(x, y, z, (long p, List u, String v) -> {p.doSomething(); blah = 1;});");
        buildAntlrTree("call(x, y, z, (int p, u, int v) -> {p.doSomething(); blah = 1;});");
        buildAntlrTree("call(x, (int p, u, int v) -> {p.doSomething(); blah = 1;}, z," +
            " (int p, u, int v) -> {p.doSomething(); blah = 1;}, 'test');");
    }
}
