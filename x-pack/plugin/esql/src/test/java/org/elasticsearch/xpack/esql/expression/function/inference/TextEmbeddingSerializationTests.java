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
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.junit.Before;

import java.io.IOException;

/** Tests serialization/deserialization of TEXT_EMBEDDING function instances. */
public class TextEmbeddingSerializationTests extends AbstractExpressionSerializationTests<TextEmbedding> {

    @Before
    public void checkCapability() {
        assumeTrue("TEXT_EMBEDDING is not enabled", EsqlCapabilities.Cap.TEXT_EMBEDDING_FUNCTION.isEnabled());
    }

    @Override
    protected TextEmbedding createTestInstance() {
        Source source = randomSource();
        Expression inputText = randomChild();
        Expression inferenceId = randomChild();
        return new TextEmbedding(source, inputText, inferenceId);
    }

    @Override
    protected TextEmbedding mutateInstance(TextEmbedding instance) throws IOException {
        Source source = instance.source();
        Expression inputText = instance.inputText();
        Expression inferenceId = instance.inferenceId();
        if (randomBoolean()) {
            inputText = randomValueOtherThan(inputText, AbstractExpressionSerializationTests::randomChild);
        } else {
            inferenceId = randomValueOtherThan(inferenceId, AbstractExpressionSerializationTests::randomChild);
        }
        return new TextEmbedding(source, inputText, inferenceId);
    }
}
