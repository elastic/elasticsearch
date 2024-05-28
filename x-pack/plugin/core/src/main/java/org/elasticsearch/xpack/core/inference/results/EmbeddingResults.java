/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.inference.InferenceServiceResults;

import java.util.List;

public interface EmbeddingResults {
    List<? extends Embedding<?>> embeddings();

    EmbeddingType embeddingType();

    enum EmbeddingType {
        SPARSE {
            public Class<? extends InferenceServiceResults> matchedClass() {
                return SparseEmbeddingResults.class;
            };
        },
        FLOAT {
            public Class<? extends InferenceServiceResults> matchedClass() {
                return TextEmbeddingResults.class;
            };
        },

        BYTE {
            public Class<? extends InferenceServiceResults> matchedClass() {
                return TextEmbeddingByteResults.class;
            };
        };

        public abstract Class<? extends InferenceServiceResults> matchedClass();
    }
}
