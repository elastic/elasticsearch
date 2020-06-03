/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;

public class TomlFoldTests extends ESTestCase {
    protected static final String PARAM_FORMATTING = "%1$s.test -> %2$s";

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {
        List<EqlFoldSpec> foldSpecs = EqlFoldSpecLoader.load("/test_folding.toml");
        foldSpecs.addAll(EqlFoldSpecLoader.load("/test_string_functions.toml"));
        List<EqlFoldSpec> unsupportedSpecs = EqlFoldSpecLoader.load("/test_unsupported.toml");

        HashSet<EqlFoldSpec> filteredSpecs = new HashSet<>(foldSpecs);
        filteredSpecs.removeAll(unsupportedSpecs);
        return asArray(filteredSpecs);
    }

    public static List<Object[]> asArray(Collection<EqlFoldSpec> specs) {
        AtomicInteger counter = new AtomicInteger();
        return specs.stream().map(spec -> new Object[] {
            counter.incrementAndGet(), spec
        }).collect(toList());
    }

    private static final EqlConfiguration caseSensitiveConfig = config(true);
    private static final EqlConfiguration caseInsensitiveConfig = config(false);
    private static EqlParser parser = new EqlParser();
    private static final EqlFunctionRegistry functionRegistry = new EqlFunctionRegistry();

    private final int num;
    private final EqlFoldSpec spec;

    private static EqlConfiguration config(boolean isCaseSensitive) {
        return new EqlConfiguration(new String[]{"none"},
            org.elasticsearch.xpack.ql.util.DateUtils.UTC, "nobody", "cluster",
            null, TimeValue.timeValueSeconds(30), -1, false, isCaseSensitive, "",
            new TaskId("foobarbaz", 1234567890), () -> false);
    }

    public TomlFoldTests(int num, EqlFoldSpec spec) {
        this.num = num;
        this.spec = spec;
    }

    private void testWithConfig(EqlConfiguration testConfig, EqlFoldSpec spec) {
        Expression expr = parser.createExpression(spec.expression());

        // resolve all function calls
        expr = expr.transformUp(e -> {
            if (e instanceof UnresolvedFunction) {
                UnresolvedFunction uf = (UnresolvedFunction) e;

                if (uf.analyzed()) {
                    return uf;
                }

                String name = uf.name();

                if (uf.childrenResolved() == false) {
                    return uf;
                }

                String functionName = functionRegistry.resolveAlias(name);
                if (functionRegistry.functionExists(functionName) == false) {
                    return uf.missing(functionName, functionRegistry.listFunctions());
                }
                FunctionDefinition def = functionRegistry.resolveFunction(functionName);
                Function f = uf.buildResolved(testConfig, def);
                return f;
            }
            return e;
        });

        assertTrue(expr.foldable());
        Object folded = expr.fold();

        // upgrade to a long, because the parser typically downgrades Long -> Integer when possible
        if (folded instanceof Integer) {
            folded  = ((Integer) folded).longValue();
        }

        assertEquals(spec.expected(), folded);
    }

    public void test() {
        if (this.spec.supportsCaseSensitive()) {
            testWithConfig(caseSensitiveConfig, spec);
        }

        if (this.spec.supportsCaseInsensitive()) {
            testWithConfig(caseInsensitiveConfig, spec);
        }
    }
}
