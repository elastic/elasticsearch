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
import org.junit.Ignore;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryableExpressionTests extends ScriptTestCase {

    public void testIntConst() {
        QueryableExpression qe = qe("emit(100)");

        assertEquals(qe.toString(), "100");
    }
    
    public void testLongMathExpression() {
        QueryableExpression qe = qe("emit(1l + 10l)");

        assertEquals(qe.toString(), "11");
    }

    @Ignore
    public void testFieldAccess() {
        QueryableExpression qe = qe("emit(doc['a'].value)");

        assertEquals(qe.toString(), "a");
    }

    public void testComplexStatement() {
        QueryableExpression qe = qe("for(int i = 0; i < 10; i++) { emit(1l) }");

        assertEquals(qe, QueryableExpression.UNQUERYABLE);
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
