/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class CohereEmbeddingTypeTests extends AbstractWireSerializingTestCase<CohereEmbeddingType> {
    public static CohereEmbeddingType createRandom() {
        return randomFrom(CohereEmbeddingType.values());
    }

    @Override
    protected Writeable.Reader<CohereEmbeddingType> instanceReader() {
        return CohereEmbeddingType::fromStream;
    }

    @Override
    protected CohereEmbeddingType createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereEmbeddingType mutateInstance(CohereEmbeddingType instance) throws IOException {
        return null;
    }
}
