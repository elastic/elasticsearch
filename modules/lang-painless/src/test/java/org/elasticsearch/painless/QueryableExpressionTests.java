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
import org.elasticsearch.queryableexpression.QueryableExpression;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class QueryableExpressionTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.long_field.txt"));
        return Collections.singletonMap(LongFieldScript.CONTEXT, whitelists);
    }

    public QueryableExpression qe(String script) {
        return scriptEngine.compile("qe_test", script, LongFieldScript.CONTEXT, Collections.emptyMap())
            .emitExpression()
            .undelay(null, null);
    }

    public void testIntConst() {
        assertEquals("100", qe("emit(100)").toString());
    }

    public void testLongMathExpression() {
        assertEquals("11", qe("emit(1l + 10l)").toString());
    }

    @AwaitsFix(bugUrl = "plaid")
    public void testFieldAccess() {
        assertEquals("a", qe("emit(doc['a'].value)").toString());
    }

    public void testComplexStatement() {
        assertEquals(QueryableExpression.UNQUERYABLE, qe("for(int i = 0; i < 10; i++) { emit(1l) }"));
    }
}
