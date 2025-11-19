/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

@FunctionName("v_l1_norm")
public class L1NormSimilarityTests extends AbstractVectorSimilarityFunctionTestCase {

    public L1NormSimilarityTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @Override
    public String getBaseEvaluatorName() {
        return L1Norm.class.getSimpleName();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return similarityParameters(L1Norm.class.getSimpleName(), L1Norm.SIMILARITY_FUNCTION);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new L1Norm(source, args.get(0), args.get(1));
    }
}
