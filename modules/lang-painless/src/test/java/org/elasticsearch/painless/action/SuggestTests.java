/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.Token;
import org.elasticsearch.painless.ScriptTestCase;
import org.elasticsearch.painless.action.PainlessExecuteAction.PainlessTestScript;
import org.elasticsearch.painless.antlr.EnhancedSuggestLexer;
import org.elasticsearch.painless.antlr.SuggestLexer;

import java.util.List;

public class SuggestTests extends ScriptTestCase {

    private List<? extends Token> getSuggestTokens(String source) {
        ANTLRInputStream stream = new ANTLRInputStream(source);
        SuggestLexer lexer = new EnhancedSuggestLexer(stream, scriptEngine.getContextsToLookups().get(PainlessTestScript.CONTEXT));
        lexer.removeErrorListeners();
        return lexer.getAllTokens();
    }

    private void compareTokens(List<? extends Token> tokens, String... expected) {
        assertEquals(expected.length % 2, 0);
        assertEquals(tokens.size(), expected.length / 2);

        int index = 0;
        for (Token token : tokens) {
            assertEquals(SuggestLexer.VOCABULARY.getDisplayName(token.getType()), expected[index++]);
            assertEquals(token.getText(), expected[index++]);
        }
    }

    public void testSuggestLexer() {
        compareTokens(getSuggestTokens("test"), SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID), "test");

        compareTokens(
            getSuggestTokens("int test;"),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "int",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";"
        );

        compareTokens(
            getSuggestTokens("ArrayList test;"),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "ArrayList",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";"
        );

        compareTokens(
            getSuggestTokens("def test;"),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "def",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";"
        );

        compareTokens(
            getSuggestTokens("int[] test;"),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ATYPE),
            "int[]",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";"
        );

        compareTokens(
            getSuggestTokens("ArrayList[] test;"),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ATYPE),
            "ArrayList[]",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";"
        );

        compareTokens(
            getSuggestTokens("def[] test;"),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ATYPE),
            "def[]",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";"
        );

        compareTokens(
            getSuggestTokens("List test = new ArrayList(); test."),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "List",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ASSIGN),
            "=",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.NEW),
            "new",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "ArrayList",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.LP),
            "(",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.RP),
            ")",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.DOT),
            "."
        );

        compareTokens(
            getSuggestTokens("List test = new ArrayList(); test.add"),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "List",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ASSIGN),
            "=",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.NEW),
            "new",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "ArrayList",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.LP),
            "(",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.RP),
            ")",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.DOT),
            ".",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.DOTID),
            "add"
        );

        compareTokens(
            getSuggestTokens("List test = new ArrayList(); test.add("),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "List",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ASSIGN),
            "=",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.NEW),
            "new",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "ArrayList",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.LP),
            "(",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.RP),
            ")",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.DOT),
            ".",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.DOTID),
            "add",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.LP),
            "("
        );

        compareTokens(
            getSuggestTokens("def test(int param) {return param;} test(2);"),
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "def",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.LP),
            "(",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.TYPE),
            "int",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "param",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.RP),
            ")",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.LBRACK),
            "{",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.RETURN),
            "return",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "param",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.RBRACK),
            "}",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.ID),
            "test",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.LP),
            "(",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.INTEGER),
            "2",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.RP),
            ")",
            SuggestLexer.VOCABULARY.getDisplayName(SuggestLexer.SEMICOLON),
            ";"
        );
    }
}
