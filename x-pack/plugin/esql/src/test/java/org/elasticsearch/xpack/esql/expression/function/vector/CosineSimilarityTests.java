/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

@FunctionName("v_cosine")
public class CosineSimilarityTests extends AbstractVectorSimilarityFunctionTestCase {

    public CosineSimilarityTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return similarityParameters(CosineSimilarity.class.getSimpleName(), CosineSimilarity.SIMILARITY_FUNCTION);
    }

    protected EsqlCapabilities.Cap capability() {
        return EsqlCapabilities.Cap.COSINE_VECTOR_SIMILARITY_FUNCTION;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new CosineSimilarity(source, args.get(0), args.get(1));
    }
}
