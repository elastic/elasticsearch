/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;

import java.util.Set;

class DenseVectorMapperConfigurator {
    private static final Set<TaskType> SUPPORTED_TASK_TYPES = Set.of(TaskType.EMBEDDING, TaskType.TEXT_EMBEDDING);

    interface DefaultElementTypeResolver {
        DenseVectorFieldMapper.ElementType resolve(IndexVersion indexVersion, DenseVectorFieldMapper.ElementType modelElementType);
    }

    interface SimilarityResolver {
        DenseVectorFieldMapper.VectorSimilarity resolve(IndexVersion indexVersion, SimilarityMeasure modelSimilarity);
    }

    interface DefaultIndexOptionsResolver {
        DenseVectorFieldMapper.DenseVectorIndexOptions resolve(IndexVersion indexVersion, MinimalServiceSettings modelSettings);
    }

    private final DefaultElementTypeResolver defaultElementTypeResolver;
    private final SimilarityResolver similarityResolver;
    private final DefaultIndexOptionsResolver defaultIndexOptionsResolver;

    DenseVectorMapperConfigurator(
        DefaultElementTypeResolver defaultElementTypeResolver,
        SimilarityResolver similarityResolver,
        DefaultIndexOptionsResolver defaultIndexOptionsResolver
    ) {
        this.defaultElementTypeResolver = defaultElementTypeResolver;
        this.similarityResolver = similarityResolver;
        this.defaultIndexOptionsResolver = defaultIndexOptionsResolver;
    }

    void configure(
        DenseVectorFieldMapper.Builder builder,
        IndexVersion indexVersion,
        MinimalServiceSettings modelSettings,
        @Nullable ExtendedDenseVectorIndexOptions extendedIndexOptions
    ) {
        if (SUPPORTED_TASK_TYPES.contains(modelSettings.taskType()) == false) {
            throw new IllegalStateException("Unsupported task type [" + modelSettings.taskType() + "]");
        }

        assert modelSettings.dimensions() != null;
        int dimensions = modelSettings.dimensions();
        DenseVectorFieldMapper.ElementType resolvedElementType = defaultElementTypeResolver.resolve(
            indexVersion,
            modelSettings.elementType()
        );
        DenseVectorFieldMapper.VectorSimilarity similarity = similarityResolver.resolve(indexVersion, modelSettings.similarity());
        DenseVectorFieldMapper.DenseVectorIndexOptions resolvedIndexOptions = null;

        if (extendedIndexOptions != null) {
            if (extendedIndexOptions.getElementType() != null) {
                resolvedElementType = extendedIndexOptions.getElementType();
            }
            resolvedIndexOptions = extendedIndexOptions.getBaseIndexOptions();
        }
        if (resolvedIndexOptions == null) {
            resolvedIndexOptions = defaultIndexOptionsResolver.resolve(indexVersion, modelSettings);
        }

        builder.dimensions(dimensions);
        builder.elementType(resolvedElementType);
        if (similarity != null) {
            builder.similarity(similarity);
        }
        if (resolvedIndexOptions != null) {
            resolvedIndexOptions.validate(resolvedElementType, dimensions, true);
            builder.indexOptions(resolvedIndexOptions);
        }
    }
}
