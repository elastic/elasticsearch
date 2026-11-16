/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.junit.Before;

import java.util.List;

/**
 * Tests error conditions and type validation for EMBEDDING function.
 */
public class EmbeddingErrorTests extends AbstractInferenceFunctionErrorTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Embedding function must be enabled", EsqlCapabilities.Cap.EMBEDDING_FUNCTION.isEnabled());
    }

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(EmbeddingTests.parameters());
    }

    @Override
    protected Expression buildFunction(Source source, Expression inputValue, Expression inferenceId, Expression options) {
        return new Embedding(source, inputValue, inferenceId, options);
    }
}
