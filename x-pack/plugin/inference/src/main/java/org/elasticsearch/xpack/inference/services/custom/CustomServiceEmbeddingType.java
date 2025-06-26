/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Locale;

public enum CustomServiceEmbeddingType {
    /**
     * Use this when you want to get back the default float embeddings.
     */
    FLOAT(DenseVectorFieldMapper.ElementType.FLOAT),
    /**
     * Use this when you want to get back signed int8 embeddings.
     */
    BYTE(DenseVectorFieldMapper.ElementType.BYTE),
    /**
     * Use this when you want to get back binary embeddings.
     */
    BIT(DenseVectorFieldMapper.ElementType.BIT),
    /**
     * This is a synonym for BIT
     */
    BINARY(DenseVectorFieldMapper.ElementType.BIT);

    private final DenseVectorFieldMapper.ElementType elementType;

    CustomServiceEmbeddingType(DenseVectorFieldMapper.ElementType elementType) {
        this.elementType = elementType;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public DenseVectorFieldMapper.ElementType toElementType() {
        return elementType;
    }

    public static CustomServiceEmbeddingType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
