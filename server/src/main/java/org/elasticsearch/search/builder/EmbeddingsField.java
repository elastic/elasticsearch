/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.VectorType;

import java.io.IOException;

import static org.elasticsearch.search.builder.SearchSourceBuilder.SEARCH_SOURCE_EMBEDDINGS_FIELDS;

public record EmbeddingsField(String field, @Nullable VectorType vectorType) implements Writeable {
    public EmbeddingsField(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalEnum(VectorType.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(SEARCH_SOURCE_EMBEDDINGS_FIELDS) == false) {
            throw new IllegalStateException("Cannot serialize EmbeddingsField with transport version [" + out.getTransportVersion() + "]");
        }
        out.writeString(field);
        out.writeOptionalEnum(vectorType);
    }
}
