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
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.TypesTests;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;

public class VerifierTests extends ESTestCase {

    private static final String INDEX_NAME = "test";
    private static final EsqlParser parser = new EsqlParser();
    private final IndexResolution defaultIndex = loadIndexResolution("mapping-basic.json");
    private final Analyzer defaultAnalyzer = new Analyzer(
        new AnalyzerContext(TEST_CFG, new EsqlFunctionRegistry(), defaultIndex),
        new Verifier()
    );

    public void testIncompatibleTypesInMathOperation() {
        assertEquals(
            "1:40: second argument of [a + c] must be [numeric], found value [c] type [keyword]",
            error("row a = 1, b = 2, c = \"xxx\" | eval y = a + c")
        );
        assertEquals(
            "1:40: second argument of [a - c] must be [numeric], found value [c] type [keyword]",
            error("row a = 1, b = 2, c = \"xxx\" | eval y = a - c")
        );
    }

    public void testRoundFunctionInvalidInputs() {
        assertEquals(
            "1:31: first argument of [round(b, 3)] must be [numeric], found value [b] type [keyword]",
            error("row a = 1, b = \"c\" | eval x = round(b, 3)")
        );
        assertEquals(
            "1:31: first argument of [round(b)] must be [numeric], found value [b] type [keyword]",
            error("row a = 1, b = \"c\" | eval x = round(b)")
        );
        assertEquals(
            "1:31: second argument of [round(a, b)] must be [integer], found value [b] type [keyword]",
            error("row a = 1, b = \"c\" | eval x = round(a, b)")
        );
        assertEquals(
            "1:31: second argument of [round(a, 3.5)] must be [integer], found value [3.5] type [double]",
            error("row a = 1, b = \"c\" | eval x = round(a, 3.5)")
        );
        assertEquals(
            "1:9: second argument of [round(123.45, \"1\")] must be [integer], found value [\"1\"] type [keyword]",
            error("row a = round(123.45, \"1\")")
        );
    }

    public void testLengthFunctionInvalidInputs() {
        assertEquals(
            "1:22: first argument of [length(a)] must be [keyword], found value [a] type [integer]",
            error("row a = 1 | eval x = length(a)")
        );
        assertEquals(
            "1:22: first argument of [length(123)] must be [keyword], found value [123] type [integer]",
            error("row a = 1 | eval x = length(123)")
        );
    }

    public void testAggsExpressionsInStatsAggs() {
        assertEquals(
            "1:44: expected an aggregate function or group but got [salary] of type [FieldAttribute]",
            error("from test | eval z = 2 | stats x = avg(z), salary by emp_no")
        );
        assertEquals(
            "1:19: expected an aggregate function or group but got [length(gender)] of type [Length]",
            error("from test | stats length(gender), count(1) by gender")
        );
        assertEquals(
            "1:19: aggregate function's parameters must be an attribute or literal; found [emp_no / 2] of type [Div]",
            error("from test | stats x = avg(emp_no / 2) by emp_no")
        );
        assertEquals(
            "1:19: aggregate function's parameters must be an attribute or literal; found [avg(gender)] of type [Avg]",
            error("from test | stats count(avg(gender)) by gender")
        );
        assertEquals(
            "1:19: aggregate function's parameters must be an attribute or literal; found [length(gender)] of type [Length]",
            error("from test | stats count(length(gender)) by gender")
        );
        assertEquals(
            "1:23: expected an aggregate function or group but got [emp_no + avg(emp_no)] of type [Add]",
            error("from test | stats x = emp_no + avg(emp_no) by emp_no")
        );
    }

    private String error(String query) {
        return error(query, defaultAnalyzer);
    }

    private String error(String query, Analyzer analyzer) {
        VerificationException e = expectThrows(VerificationException.class, () -> analyzer.analyze(parser.createStatement(query)));
        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    private static IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(new EsIndex(INDEX_NAME, TypesTests.loadMapping(name)));
    }
}
