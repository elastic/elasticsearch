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
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class QueryableExpressionTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.long_field.txt"));
        return Collections.singletonMap(LongFieldScript.CONTEXT, whitelists);
    }

    public QueryableExpression qe(String script) {
        return qe(script, null);
    }

    public QueryableExpression qe(String script, Function<String, QueryableExpression> lookup) {
        return scriptEngine.compile("qe_test", script, LongFieldScript.CONTEXT, Collections.emptyMap())
            .emitExpression()
            .build(lookup, null);
    }

    public void testIntConst() {
        assertEquals("100", qe("emit(100)").toString());
    }

    public void testLongMathExpression() {
        assertEquals("11", qe("emit(1l + 10l)").toString());
    }

    private Function<String, QueryableExpression> longLookup = (field) -> LongQueryableExpression.field(
        field,
        (LongQueryableExpression.LongQueries) null
    );

    public void testFieldRef1() {
        assertEquals("a", qe("emit(doc['a'].value)", longLookup).toString());
    }

    public void testFieldRef2() {
        assertEquals("a", qe("emit(doc['a'].getValue())", longLookup).toString());
    }

    public void testFieldRef3() {
        assertEquals("a", qe("emit(doc.get('a').value)", longLookup).toString());
    }

    public void testFieldRef4() {
        assertEquals("a", qe("emit(doc.get('a').getValue())", longLookup).toString());
    }

    public void testFieldRefPlusLong() {
        assertEquals("a + 1", qe("emit(doc['a'].value + 1l)", longLookup).toString());
    }

    public void testFieldRefOverLong() {
        assertEquals("b / 10", qe("emit(doc['b'].value / 10l)", longLookup).toString());
    }

    @AwaitsFix(bugUrl = "Terms with multiple operations cannot be approximated yet")
    public void testCelsiusToFahrenheitLong() {
        assertEquals("(temp_c * 2) + 32", qe("emit((doc['temp_c'].value * 2l) + 32l)", longLookup).toString());
    }

    public void testComplexStatement() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("for(int i = 0; i < 10; i++) { emit(1l) }"));
    }

    public void testConstAssignment() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("def one = 1l; emit(one + 10l)"));
    }

    public void testIf() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("if (1 > 2) { emit(100) }"));
    }

    public void testTernary() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("emit(1 > 2 ? 100 : 10)"));
    }
}
