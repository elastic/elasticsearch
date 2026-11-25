/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;

/**
 * Tests error conditions and type validation for TEXT_EMBEDDING function.
 */
public class TextEmbeddingErrorTests extends AbstractInferenceFunctionErrorTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TextEmbeddingTests.parameters());
    }

    @Override
    protected Expression buildFunction(Source source, Expression inputText, Expression inferenceId, Expression options) {
        return new TextEmbedding(source, inputText, inferenceId, options);
    }
}
