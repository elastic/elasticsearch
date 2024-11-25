/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.junit.BeforeClass;

import java.util.List;
import java.util.function.Supplier;

public class KqlTests extends NoneFieldFullTextFunctionTestCase {
    @BeforeClass
    protected static void ensureKqlFunctionEnabled() {
        assumeTrue("kql function capability not available", EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled());
    }

    public KqlTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return generateParameters();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Kql(source, args.get(0));
    }
}
