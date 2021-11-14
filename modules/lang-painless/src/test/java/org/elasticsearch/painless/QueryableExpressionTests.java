/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.queryableexpression.QueryableExpression;
import org.elasticsearch.script.LongFieldScript;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryableExpressionTests extends ScriptTestCase {

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

    private QueryableExpression qe(String script) {
        PainlessLookup lookup = PainlessLookupBuilder.buildFromWhitelists(
            Stream.concat(
                PainlessPlugin.BASE_WHITELISTS.stream(),
                Stream.of(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.long_field.txt"))
            ).collect(Collectors.toList())
        );

        return new Compiler(LongFieldScript.class, null, null, lookup).compileQueryableExpression(
            "testScript",
            script,
            new CompilerSettings()
        );
    }

}
