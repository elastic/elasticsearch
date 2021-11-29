/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.queryableexpression.LongQueryableExpression;
import org.elasticsearch.queryableexpression.QueryableExpression;
import org.elasticsearch.queryableexpression.StringQueryableExpression;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.StringFieldScript;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CollectArgumentTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        return Map.ofEntries(
            Map.entry(LongFieldScript.CONTEXT, whitelists("long")),
            Map.entry(StringFieldScript.CONTEXT, whitelists("keyword"))
        );
    }

    private List<Whitelist> whitelists(String fieldType) {
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script." + fieldType + "_field.txt"));
        return whitelists;
    }

    public QueryableExpression qe(String script) {
        return qe(script, null);
    }

    public QueryableExpression qe(String script, Function<String, QueryableExpression> lookup) {
        return qe(script, lookup, null);
    }

    public QueryableExpression qe(String script, Function<String, QueryableExpression> lookup, Function<String, Object> params) {
        return scriptEngine.compile("qe_test", script, LongFieldScript.CONTEXT, Map.of(CompilerSettings.COLLECT_ARGUMENTS, "true"))
            .emitExpression()
            .build(lookup, params);
    }

    public void testIntConst() {
        assertEquals("100", qe("emit(100)").toString());
    }

    public void testLongConst() {
        assertEquals("100", qe("emit(100L)").toString());
    }

    public void testIntMathExpression() {
        assertEquals("11", qe("emit(1 + 10)").toString());
    }

    public void testLongMathExpression() {
        assertEquals("11", qe("emit(1L + 10L)").toString());
    }

    private final Function<String, QueryableExpression> longLookup = (field) -> LongQueryableExpression.field(
        field,
        (LongQueryableExpression.LongQueries) null
    );

    public void testFieldRef1() {
        assertEquals("a", qe("emit(doc['a'].value)", longLookup).toString());
        assertEquals("a", qe("def a = doc['a'].value; emit(a)", longLookup).toString());
        assertEquals("a", qe("def a; a = doc['a'].value; emit(a)", longLookup).toString());
    }

    public void testFieldRef2() {
        assertEquals("a", qe("emit(doc['a'].getValue())", longLookup).toString());
        assertEquals("a", qe("def a = doc['a'].getValue(); emit(a)", longLookup).toString());
        assertEquals("a", qe("def a; a = doc['a'].getValue(); emit(a)", longLookup).toString());
    }

    public void testFieldRef3() {
        assertEquals("a", qe("emit(doc.get('a').value)", longLookup).toString());
        assertEquals("a", qe("def a = doc.get('a').value; emit(a)", longLookup).toString());
        assertEquals("a", qe("def a; a = doc.get('a').value; emit(a)", longLookup).toString());
    }

    public void testFieldRef4() {
        assertEquals("a", qe("emit(doc.get('a').getValue())", longLookup).toString());
        assertEquals("a", qe("def a = doc.get('a').getValue(); emit(a)", longLookup).toString());
        assertEquals("a", qe("def a; a = doc.get('a').getValue(); emit(a)", longLookup).toString());
    }

    public void testFieldRef5() {
        assertEquals("a", qe("emit(doc.a.value)", longLookup).toString());
        assertEquals("a", qe("def a = doc.a.value; emit(a)", longLookup).toString());
        assertEquals("a", qe("def a; a = doc.a.value; emit(a)", longLookup).toString());
    }

    public void testFieldRef6() {
        assertEquals("a", qe("emit(doc.a.getValue())", longLookup).toString());
        assertEquals("a", qe("def a = doc.a.getValue(); emit(a)", longLookup).toString());
        assertEquals("a", qe("def a; a = doc.a.getValue(); emit(a)", longLookup).toString());
    }

    public void testFieldRefPlusInt() {
        assertEquals("a + 1", qe("emit(doc['a'].value + 1)", longLookup).toString());
    }

    public void testFieldRefPlusLong() {
        assertEquals("a + 1", qe("emit(doc['a'].value + 1L)", longLookup).toString());
    }

    public void testFieldRefOverInt() {
        assertEquals("b / 10", qe("emit(doc['b'].value / 10)", longLookup).toString());
    }

    public void testFieldRefOverLong() {
        assertEquals("b / 10", qe("emit(doc['b'].value / 10L)", longLookup).toString());
    }

    public void testLongOverFieldRef() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("emit(10l / doc['b'].value)", longLookup));
    }

    public void testCelsiusToFahrenheitLong() {
        assertEquals("temp_c * 5 / 9 + 32", qe("emit(((doc['temp_c'].value * 5) / 9) + 32l)", longLookup).toString());
    }

    public void testFahrenheitToCelsiusLong() {
        assertEquals("temp_f + -32 * 5 / 9", qe("emit(((doc['temp_f'].value - 32) * 5) / 9)", longLookup).toString());
    }

    private static final Function<String, Object> X_IS_100L = Map.of("x", 100L)::get;

    public void testParamsMapStyle() {
        assertEquals("1000", qe("emit(params['x'] * 10L)", null, X_IS_100L).toString());
    }

    public void testParamsMethodStyle() {
        assertEquals("1000", qe("emit(params.get('x') * 10L)", null, X_IS_100L).toString());
    }

    public void testParamsVarStyle() {
        assertEquals("1000", qe("emit(params.x * 10L)", null, X_IS_100L).toString());
    }

    public void testParamsLongPlusLongOverflowing() {
        // TODO support Long.MAX_VALUE in the script
        assertEquals(Long.toString(100L + Long.MAX_VALUE), qe("emit(params.x + " + Long.MAX_VALUE + "L)", null, X_IS_100L).toString());
    }

    private static final Function<String, Object> X_IS_100 = Map.of("x", 100)::get;

    public void testParamsIntTimesInt() {
        assertEquals("1000", qe("emit(params.x * 10)", null, X_IS_100).toString());
    }

    public void testParamsIntPlusIntOverflowing() {
        // TODO support Integer.MAX_VALUE in the script
        assertEquals(
            Integer.toString(100 + Integer.MAX_VALUE),
            qe("emit(params.x + " + Integer.MAX_VALUE + ")", null, X_IS_100).toString()
        );
    }

    public void testParamsIntTimesLong() {
        assertEquals("1000", qe("emit(params.x * 10L)", null, X_IS_100).toString());
    }

    public void testParamsMissing() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("emit(params.y * 10L)", null, X_IS_100L));
    }

    public void testParamPlusField() {
        assertEquals("a + 100", qe("emit(doc.a.value + params.x)", longLookup, X_IS_100L).toString());
    }

    private static final Function<String, QueryableExpression> STRING_LOOKUP = field -> StringQueryableExpression.field(field, null);

    public void testString() {
        assertEquals("a", qe("emit(doc.a.value)", STRING_LOOKUP).toString());
    }

    public void testSubstring() {
        assertEquals(
            "a.substring()",
            scriptEngine.compile(
                "qe_test",
                "emit(doc.a.value.substring(0, 5))",
                StringFieldScript.CONTEXT,
                Map.of(CompilerSettings.COLLECT_ARGUMENTS, "true")
            ).emitExpression().build(STRING_LOOKUP, null).toString()
        );
    }

    public void testFor() {
        assertEquals("1", qe("for(int i = 0; i < doc.a.value; i++) { emit(1L) }", longLookup).toString());
    }

    public void testConstAssignment() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("def one = 1L; emit(one + 10L)"));
    }

    public void testIf() {
        assertEquals("a", qe("if (doc.b.value > 2) { emit(doc.a.value) }", longLookup).toString());
        assertEquals("a", qe("def a = doc.a.value; if (a > 2) { emit(a) }", longLookup).toString());
    }

    public void testTernary() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("emit(1 > 2 ? 100 : 10)"));
    }

    public void testEmitTwice() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("emit(1); emit(2);"));
    }

    public void testAssignment() {
        assertEquals(
            QueryableExpression.UNQUERYABLE,
            qe("doc = new HashMap(); emit(doc.a.value + params.x)", longLookup, (param) -> param.equals("x") ? 100L : null)
        );
        assertEquals(
            QueryableExpression.UNQUERYABLE,
            qe("params = new HashMap(); emit(doc.a.value + params.x)", longLookup, (param) -> param.equals("x") ? 100L : null)
        );
    }

    public void testUnknownMethodCall() {
        assertEquals("unknown(a)", qe("emit(doc.a.value.toString())", longLookup).toString());
    }

    public void testUnknownMethodCallArg() {
        assertEquals("unknown(a)", qe("emit(Long.valueOf(doc.a.value))", longLookup).toString());
    }

    public void testUnknownBinaryOp() {
        assertEquals("unknown(a, 5)", qe("emit(doc.a.value % 5)", longLookup).toString());
    }

    public void testUnknownBinaryOpWithTwoFields() {
        assertEquals("unknown(a, b)", qe("emit(doc.a.value % doc.b.value)", longLookup).toString());
    }

    @AwaitsFix(bugUrl = "plaid")
    public void testUnknownBinaryOpWithThreeFields() {
        assertEquals("unknown(a, b)", qe("emit((doc.a.value + doc.b.value) % doc.c.value)", longLookup).toString());
    }
}
