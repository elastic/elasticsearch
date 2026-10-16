/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Fluent builder for the parameters of a {@code dense_vector} field mapping; unset parameters are omitted.
 */
class DenseVectorMappingBuilder implements Cloneable {
    @Nullable
    private Integer dims;
    @Nullable
    private Boolean index;
    @Nullable
    private VectorSimilarity similarity;
    @Nullable
    private ElementType elementType;
    @Nullable
    private Map<String, Object> indexOptions;
    @Nullable
    private CheckedConsumer<XContentBuilder, IOException> additionalParams;

    DenseVectorMappingBuilder dims(@Nullable Integer dims) {
        this.dims = dims;
        return this;
    }

    DenseVectorMappingBuilder index(@Nullable Boolean index) {
        this.index = index;
        return this;
    }

    DenseVectorMappingBuilder similarity(@Nullable VectorSimilarity similarity) {
        this.similarity = similarity;
        return this;
    }

    DenseVectorMappingBuilder elementType(@Nullable ElementType elementType) {
        this.elementType = elementType;
        return this;
    }

    DenseVectorMappingBuilder indexOptions(@Nullable Map<String, Object> indexOptions) {
        this.indexOptions = indexOptions;
        return this;
    }

    DenseVectorMappingBuilder additionalParams(@Nullable CheckedConsumer<XContentBuilder, IOException> additionalParams) {
        this.additionalParams = additionalParams;
        return this;
    }

    @Override
    public DenseVectorMappingBuilder clone() {
        try {
            return (DenseVectorMappingBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Writes the configured {@code dense_vector} parameters into {@code b} and returns it.
     */
    XContentBuilder build(XContentBuilder b) throws IOException {
        b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
        if (dims != null) {
            b.field("dims", dims);
        }
        if (index != null) {
            b.field("index", index);
        }
        if (similarity != null) {
            b.field("similarity", similarity.toString());
        }
        if (elementType != null) {
            b.field("element_type", elementType.toString());
        }
        if (indexOptions != null) {
            b.field("index_options", indexOptions);
        }
        if (additionalParams != null) {
            additionalParams.accept(b);
        }
        return b;
    }
}
