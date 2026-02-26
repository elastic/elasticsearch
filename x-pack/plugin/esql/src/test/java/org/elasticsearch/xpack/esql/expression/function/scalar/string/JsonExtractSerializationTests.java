/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class JsonExtractSerializationTests extends AbstractExpressionSerializationTests<JsonExtract> {
    @Override
    protected JsonExtract createTestInstance() {
        Source source = randomSource();
        Expression jsonInput = randomChild();
        Expression path = randomChild();
        return new JsonExtract(source, jsonInput, path);
    }

    @Override
    protected JsonExtract mutateInstance(JsonExtract instance) throws IOException {
        Source source = instance.source();
        Expression jsonInput = instance.str();
        Expression path = instance.path();
        switch (between(0, 1)) {
            case 0 -> jsonInput = randomValueOtherThan(jsonInput, AbstractExpressionSerializationTests::randomChild);
            case 1 -> path = randomValueOtherThan(path, AbstractExpressionSerializationTests::randomChild);
        }
        return new JsonExtract(source, jsonInput, path);
    }
}
