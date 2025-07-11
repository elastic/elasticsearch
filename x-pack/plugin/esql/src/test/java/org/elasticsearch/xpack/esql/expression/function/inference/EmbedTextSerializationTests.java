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

public class EmbedTextSerializationTests extends AbstractExpressionSerializationTests<EmbedText> {

    @Before
    public void checkCapability() {
        assumeTrue("EMBED_TEXT is not enabled", EsqlCapabilities.Cap.EMBED_TEXT_FUNCTION.isEnabled());
    }

    @Override
    protected EmbedText createTestInstance() {
        Source source = randomSource();
        Expression inputText = randomChild();
        Expression inferenceId = randomChild();
        return new EmbedText(source, inputText, inferenceId);
    }

    @Override
    protected EmbedText mutateInstance(EmbedText instance) throws IOException {
        Source source = instance.source();
        Expression inputText = instance.inputText();
        Expression inferenceId = instance.inferenceId();
        if (randomBoolean()) {
            inputText = randomValueOtherThan(inputText, AbstractExpressionSerializationTests::randomChild);
        } else {
            inferenceId = randomValueOtherThan(inferenceId, AbstractExpressionSerializationTests::randomChild);
        }
        return new EmbedText(source, inputText, inferenceId);
    }
}
