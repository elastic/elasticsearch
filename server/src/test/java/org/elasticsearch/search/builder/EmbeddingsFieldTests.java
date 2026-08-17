/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class EmbeddingsFieldTests extends AbstractWireSerializingTestCase<EmbeddingsField> {

    public static VectorType randomVectorType() {
        return randomBoolean() ? null : randomFrom(VectorType.values());
    }

    public static EmbeddingsField randomEmbeddingsField() {
        return new EmbeddingsField(randomAlphaOfLengthBetween(5, 10), randomVectorType());
    }

    @Override
    protected Writeable.Reader<EmbeddingsField> instanceReader() {
        return EmbeddingsField::new;
    }

    @Override
    protected EmbeddingsField createTestInstance() {
        return randomEmbeddingsField();
    }

    @Override
    protected EmbeddingsField mutateInstance(EmbeddingsField instance) {
        String field = instance.field();
        VectorType vectorType = instance.vectorType();
        switch (randomIntBetween(0, 1)) {
            case 0 -> field = randomValueOtherThan(field, () -> randomAlphaOfLengthBetween(5, 10));
            case 1 -> vectorType = randomValueOtherThan(vectorType, EmbeddingsFieldTests::randomVectorType);
            default -> throw new AssertionError();
        }
        return new EmbeddingsField(field, vectorType);
    }
}
