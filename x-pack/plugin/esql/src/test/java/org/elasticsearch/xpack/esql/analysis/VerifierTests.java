/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.TypedParamValue;

import java.util.ArrayList;
import java.util.List;

public class VerifierTests extends ESTestCase {

    private static final EsqlParser parser = new EsqlParser();
    private final Analyzer defaultAnalyzer = AnalyzerTestUtils.expandedDefaultAnalyzer();

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

    public void testAggsExpressionsInStatsAggs() {
        assertEquals(
            "1:44: expected an aggregate function or group but got [salary] of type [FieldAttribute]",
            error("from test | eval z = 2 | stats x = avg(z), salary by emp_no")
        );
        assertEquals(
            "1:19: expected an aggregate function or group but got [length(first_name)] of type [Length]",
            error("from test | stats length(first_name), count(1) by first_name")
        );
        assertEquals(
            "1:19: aggregate function's parameters must be an attribute or literal; found [emp_no / 2] of type [Div]",
            error("from test | stats x = avg(emp_no / 2) by emp_no")
        );
        assertEquals(
            "1:25: argument of [avg(first_name)] must be [numeric], found value [first_name] type [keyword]",
            error("from test | stats count(avg(first_name)) by first_name")
        );
        assertEquals(
            "1:19: aggregate function's parameters must be an attribute or literal; found [length(first_name)] of type [Length]",
            error("from test | stats count(length(first_name)) by first_name")
        );
        assertEquals(
            "1:23: expected an aggregate function or group but got [emp_no + avg(emp_no)] of type [Add]",
            error("from test | stats x = emp_no + avg(emp_no) by emp_no")
        );
    }

    public void testDoubleRenamingField() {
        assertEquals(
            "1:47: Column [emp_no] renamed to [r1] and is no longer available [r3 = emp_no]",
            error("from test | rename r1 = emp_no, r2 = r1, r3 = emp_no | keep r3")
        );
    }

    public void testDuplicateRenaming() {
        assertEquals(
            "1:38: Column [emp_no] renamed to [r1] and is no longer available [r1 = emp_no]",
            error("from test | rename r1 = emp_no, r1 = emp_no | keep r1")
        );
    }

    public void testDoubleRenamingReference() {
        assertEquals(
            "1:63: Column [r1] renamed to [r2] and is no longer available [r3 = r1]",
            error("from test | rename r1 = emp_no, r2 = r1, x = first_name, r3 = r1 | keep r3")
        );
    }

    public void testDropAfterRenaming() {
        assertEquals("1:39: Unknown column [emp_no]", error("from test | rename r1 = emp_no | drop emp_no"));
    }

    public void testNonStringFieldsInDissect() {
        assertEquals(
            "1:21: Dissect only supports KEYWORD values, found expression [emp_no] type [INTEGER]",
            error("from test | dissect emp_no \"%{foo}\"")
        );
    }

    public void testNonStringFieldsInGrok() {
        assertEquals(
            "1:18: Grok only supports KEYWORD values, found expression [emp_no] type [INTEGER]",
            error("from test | grok emp_no \"%{WORD:foo}\"")
        );
    }

    public void testMixedNonConvertibleTypesInIn() {
        assertEquals(
            "1:19: 2nd argument of [emp_no in (1, \"two\")] must be [integer], found value [\"two\"] type [keyword]",
            error("from test | where emp_no in (1, \"two\")")
        );
    }

    public void testSumOnDate() {
        assertEquals(
            "1:19: argument of [sum(hire_date)] must be [numeric], found value [hire_date] type [datetime]",
            error("from test | stats sum(hire_date)")
        );
    }

    public void testWrongInputParam() {
        assertEquals(
            "1:19: first argument of [emp_no == ?] is [numeric] so second argument must also be [numeric] but was [keyword]",
            error("from test | where emp_no == ?", "foo")
        );

        assertEquals(
            "1:19: first argument of [emp_no == ?] is [numeric] so second argument must also be [numeric] but was [null]",
            error("from test | where emp_no == ?", new Object[] { null })
        );
    }

    private String error(String query, Object... params) {
        return error(query, defaultAnalyzer, params);
    }

    private String error(String query, Analyzer analyzer, Object... params) {
        List<TypedParamValue> parameters = new ArrayList<>();
        for (Object param : params) {
            if (param == null) {
                parameters.add(new TypedParamValue("null", null));
            } else if (param instanceof String) {
                parameters.add(new TypedParamValue("keyword", param));
            } else if (param instanceof Number) {
                parameters.add(new TypedParamValue("param", param));
            } else {
                throw new IllegalArgumentException("VerifierTests don't support params of type " + param.getClass());
            }
        }
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzer.analyze(parser.createStatement(query, parameters))
        );
        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }
}
