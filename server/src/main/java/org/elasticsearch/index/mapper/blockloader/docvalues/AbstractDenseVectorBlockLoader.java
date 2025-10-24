/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.COSINE_MAGNITUDE_FIELD_SUFFIX;

/**
 * Abstract base class for dense vector block loaders that provides common infrastructure
 * for reading vector values from doc values. Subclasses specialize in different output
 * types (e.g., raw vectors vs similarity scores).
 */
public abstract class AbstractDenseVectorBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    protected final String fieldName;
    protected final DenseVectorFieldMapper.DenseVectorFieldType fieldType;

    protected AbstractDenseVectorBlockLoader(String fieldName, DenseVectorFieldMapper.DenseVectorFieldType fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        switch (fieldType.getElementType()) {
            case FLOAT -> {
                FloatVectorValues floatVectorValues = context.reader().getFloatVectorValues(fieldName);
                if (floatVectorValues != null) {
                    if (fieldType.isNormalized()) {
                        NumericDocValues magnitudeDocValues = context.reader()
                            .getNumericDocValues(fieldType.name() + COSINE_MAGNITUDE_FIELD_SUFFIX);
                        return createNormalizedFloatReader(floatVectorValues, magnitudeDocValues);
                    }
                    return createFloatReader(floatVectorValues);
                }
            }
            case BYTE -> {
                ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                    return createByteReader(byteVectorValues);
                }
            }
            case BIT -> {
                ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                    AllReader bitReader = createBitReader(byteVectorValues);
                    if (bitReader != null) {
                        return bitReader;
                    }
                }
            }
        }

        return new ConstantNullsReader();
    }

    /**
     * Creates a reader for float vectors without normalization.
     */
    protected abstract AllReader createFloatReader(FloatVectorValues vectorValues);

    /**
     * Creates a reader for float vectors with normalization support.
     */
    protected abstract AllReader createNormalizedFloatReader(FloatVectorValues vectorValues, NumericDocValues magnitudeDocValues);

    /**
     * Creates a reader for byte vectors.
     */
    protected abstract AllReader createByteReader(ByteVectorValues vectorValues);

    /**
     * Creates a reader for bit vectors. Returns null if not supported.
     * Default implementation returns null - only vector output loaders support BIT type.
     */
    protected AllReader createBitReader(ByteVectorValues vectorValues) {
        return null;
    }
}
