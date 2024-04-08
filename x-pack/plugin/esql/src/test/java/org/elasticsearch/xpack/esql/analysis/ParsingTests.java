/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.TypesTests;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;

public class ParsingTests extends ESTestCase {
    private static final String INDEX_NAME = "test";
    private static final EsqlParser parser = new EsqlParser();

    private final IndexResolution defaultIndex = loadIndexResolution("mapping-basic.json");
    private final Analyzer defaultAnalyzer = new Analyzer(
        new AnalyzerContext(TEST_CFG, new EsqlFunctionRegistry(), defaultIndex, emptyPolicyResolution()),
        TEST_VERIFIER
    );

    public void testCaseFunctionInvalidInputs() {
        assertEquals("1:23: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case()"));
        assertEquals("1:23: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case(a)"));
        assertEquals("1:23: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case(1)"));
    }

    public void testConcatFunctionInvalidInputs() {
        assertEquals("1:23: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat()"));
        assertEquals("1:23: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat(a)"));
        assertEquals("1:23: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat(1)"));
    }

    public void testCoalesceFunctionInvalidInputs() {
        assertEquals("1:23: error building [coalesce]: expects at least one argument", error("row a = 1 | eval x = coalesce()"));
    }

    public void testGreatestFunctionInvalidInputs() {
        assertEquals("1:23: error building [greatest]: expects at least one argument", error("row a = 1 | eval x = greatest()"));
    }

    public void testLeastFunctionInvalidInputs() {
        assertEquals("1:23: error building [least]: expects at least one argument", error("row a = 1 | eval x = least()"));
    }

    private String error(String query) {
        ParsingException e = expectThrows(ParsingException.class, () -> defaultAnalyzer.analyze(parser.createStatement(query)));
        String message = e.getMessage();
        assertTrue(message.startsWith("line "));
        return message.substring("line ".length());
    }

    private static IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(new EsIndex(INDEX_NAME, TypesTests.loadMapping(name)));
    }
}
