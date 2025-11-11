/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.histogram;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class HistogramPercentileErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Before
    public void setup() {
        assumeTrue(
            "Only when esql_exponential_histogram feature flag is enabled",
            EsqlCorePlugin.EXPONENTIAL_HISTOGRAM_FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(HistogramPercentileTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new HistogramPercentile(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(typeErrorMessage(false, validPerPosition, signature, (v, p) -> switch (p) {
            case 0 -> "exponential_histogram";
            case 1 -> "numeric types";
            default -> throw new IllegalArgumentException("Unexpected parameter position: " + p);
        }));
    }
}
