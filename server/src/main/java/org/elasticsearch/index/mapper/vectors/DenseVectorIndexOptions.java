/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;

public abstract class DenseVectorIndexOptions extends IndexOptions {

    DenseVectorIndexOptions(DenseVectorFieldMapper.VectorIndexType type) {
        super(type);
    }

    abstract KnnVectorsFormat getVectorsFormat(DenseVectorFieldMapper.ElementType elementType);

    final void validateElementType(DenseVectorFieldMapper.ElementType elementType) {
        if (((DenseVectorFieldMapper.VectorIndexType) type).supportsElementType(elementType) == false) {
            throw new IllegalArgumentException(
                "[element_type] cannot be [" + elementType.toString() + "] when using index type [" + type + "]"
            );
        }
    }

    public DenseVectorFieldMapper.VectorIndexType getDenseVectorIndexType() {
        return (DenseVectorFieldMapper.VectorIndexType) type;
    }

    public void validateDimension(int dim) {
        if (((DenseVectorFieldMapper.VectorIndexType) type).supportsDimension(dim)) {
            return;
        }
        throw new IllegalArgumentException(
            ((DenseVectorFieldMapper.VectorIndexType) type).name + " only supports even dimensions; provided=" + dim
        );
    }
}
