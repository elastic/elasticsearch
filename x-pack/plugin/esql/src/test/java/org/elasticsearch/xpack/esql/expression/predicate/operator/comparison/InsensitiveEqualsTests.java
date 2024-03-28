/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.InsensitiveBinaryComparison;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class InsensitiveEqualsTests extends AbstractInsensitiveBinaryComparisonTestCase {
    public InsensitiveEqualsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("String =~ String", () -> {
            BytesRef rhs = BytesRefs.toBytesRef(randomAlphaOfLength(5));
            BytesRef lhs = BytesRefs.toBytesRef(randomAlphaOfLength(5));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataTypes.KEYWORD, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataTypes.KEYWORD, "rhs")
                ),
                "InsensitiveEqualsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataTypes.BOOLEAN,
                equalTo(lhs.utf8ToString().toLowerCase(Locale.ROOT) == rhs.utf8ToString().toLowerCase(Locale.ROOT))
            );
        })));
    }

    @Override
    protected <T extends Comparable<T>> Matcher<Object> resultMatcher(T lhs, T rhs) {
        return equalTo(lhs.equals(rhs));
    }

    @Override
    protected InsensitiveBinaryComparison build(Source source, Expression lhs, Expression rhs) {
        return new InsensitiveEquals(source, lhs, rhs);
    }

    @Override
    protected boolean isEquality() {
        return true;
    }

    protected boolean supportsTypes(DataType lhsType, DataType rhsType) {
        return DataTypes.isString(lhsType) && DataTypes.isString(rhsType);
    }
}
