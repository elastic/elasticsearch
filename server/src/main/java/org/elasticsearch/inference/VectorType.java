/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.core.Nullable;

import java.util.Locale;

public enum VectorType {
    SPARSE_VECTOR,
    DENSE_VECTOR;

    /**
     * Returns the vector type produced by the given task type, or {@code null} if the task type does not produce embeddings.
     */
    @Nullable
    public static VectorType fromTaskType(TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> SPARSE_VECTOR;
            case TEXT_EMBEDDING, EMBEDDING -> DENSE_VECTOR;
            case RERANK, COMPLETION, CHAT_COMPLETION, ANY -> null;
        };
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
