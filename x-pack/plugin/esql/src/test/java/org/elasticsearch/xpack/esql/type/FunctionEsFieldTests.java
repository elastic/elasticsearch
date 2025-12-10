/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.vector.CosineSimilarity;
import org.elasticsearch.xpack.esql.expression.function.vector.DotProduct;
import org.elasticsearch.xpack.esql.expression.function.vector.Hamming;
import org.elasticsearch.xpack.esql.expression.function.vector.L1Norm;
import org.elasticsearch.xpack.esql.expression.function.vector.L2Norm;

import java.io.IOException;

public class FunctionEsFieldTests extends AbstractEsFieldTypeTests<FunctionEsField> {
    @Override
    protected FunctionEsField createTestInstance() {
        EsField esField = randomAnyEsField(4);
        DataType dataType = randomFrom(DataType.types());
        DenseVectorFieldMapper.VectorSimilarityFunctionConfig functionConfig = randomFunctionConfig();
        return new FunctionEsField(esField, dataType, functionConfig);
    }

    @Override
    protected FunctionEsField mutateInstance(FunctionEsField instance) throws IOException {
        EsField esField = instance.getExactField();
        DataType dataType = instance.getDataType();
        BlockLoaderFunctionConfig functionConfig = instance.functionConfig();
        switch (between(0, 2)) {
            case 0 -> esField = randomValueOtherThan(esField, () -> randomAnyEsField(4));
            case 1 -> dataType = randomValueOtherThan(dataType, () -> randomFrom(DataType.types()));
            case 2 -> functionConfig = randomValueOtherThan(functionConfig, this::randomFunctionConfig);
            default -> throw new IllegalArgumentException();
        }

        return new FunctionEsField(esField, dataType, functionConfig);
    }

    private DenseVectorFieldMapper.VectorSimilarityFunctionConfig randomFunctionConfig() {
        DenseVectorFieldMapper.SimilarityFunction similarityFunction = randomFrom(
            DotProduct.SIMILARITY_FUNCTION,
            CosineSimilarity.SIMILARITY_FUNCTION,
            L1Norm.SIMILARITY_FUNCTION,
            L2Norm.SIMILARITY_FUNCTION,
            Hamming.SIMILARITY_FUNCTION
        );
        return new DenseVectorFieldMapper.VectorSimilarityFunctionConfig(similarityFunction, randomVector());
    }

    private static float[] randomVector() {
        int dim = randomIntBetween(2, 10);
        float[] vector = new float[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    @Override
    protected FunctionEsField copyInstance(FunctionEsField instance, TransportVersion version) throws IOException {
        // Does not use serialization, as FunctionEsField is not meant to be serialized and throws if attempted
        return new FunctionEsField(instance.getExactField(), instance.getDataType(), instance.functionConfig());
    }
}
