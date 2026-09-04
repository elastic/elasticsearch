/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.IndexOptions;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ExtendedDenseVectorIndexOptions extends IndexOptions {
    public static final ParseField ELEMENT_TYPE_FIELD = new ParseField("element_type");

    private final DenseVectorFieldMapper.DenseVectorIndexOptions baseIndexOptions;
    private final DenseVectorFieldMapper.ElementType elementType;

    public ExtendedDenseVectorIndexOptions(
        @Nullable DenseVectorFieldMapper.DenseVectorIndexOptions baseIndexOptions,
        @Nullable DenseVectorFieldMapper.ElementType elementType
    ) {
        this.baseIndexOptions = baseIndexOptions;
        this.elementType = elementType;
    }

    public DenseVectorFieldMapper.DenseVectorIndexOptions getBaseIndexOptions() {
        return baseIndexOptions;
    }

    public DenseVectorFieldMapper.ElementType getElementType() {
        return elementType;
    }

    @Override
    public void toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        if (baseIndexOptions != null) {
            baseIndexOptions.toXContentFragment(builder, params);
        }
        if (elementType != null) {
            builder.field(ELEMENT_TYPE_FIELD.getPreferredName(), elementType.toString());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtendedDenseVectorIndexOptions that = (ExtendedDenseVectorIndexOptions) o;
        return Objects.equals(baseIndexOptions, that.baseIndexOptions) && Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseIndexOptions, elementType);
    }
}
