/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

public class OutlierDetectionAggregatorFactory extends AggregatorFactory {

    private final String field;
    private final int topN;
    private final int nNeighbors;
    private final Integer sampleSize;
    private final Integer projectionDim;
    private final Long seed;
    private final int overfetchFactor;
    private final OutlierDetectionMethod method;
    private final int maxCoordSample;
    private final ScoreNormalization normalize;

    public OutlierDetectionAggregatorFactory(
        String name,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        String field,
        int topN,
        int nNeighbors,
        Integer sampleSize,
        Integer projectionDim,
        Long seed,
        int overfetchFactor,
        OutlierDetectionMethod method,
        int maxCoordSample,
        ScoreNormalization normalize
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.field = field;
        this.topN = topN;
        this.nNeighbors = nNeighbors;
        this.sampleSize = sampleSize;
        this.projectionDim = projectionDim;
        this.seed = seed;
        this.overfetchFactor = overfetchFactor;
        this.method = method;
        this.maxCoordSample = maxCoordSample;
        this.normalize = normalize;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {

        MappedFieldType fieldType = context.getFieldType(field);
        if (fieldType == null) {
            throw new AggregationExecutionException(
                "field [" + field + "] not found for [" + OutlierDetectionAggregationBuilder.NAME + "] aggregation"
            );
        }
        if (fieldType instanceof DenseVectorFieldMapper.DenseVectorFieldType == false) {
            throw new AggregationExecutionException(
                "field ["
                    + field
                    + "] is not a dense_vector field; [" + OutlierDetectionAggregationBuilder.NAME + "] requires a dense_vector field"
            );
        }

        DenseVectorFieldMapper.DenseVectorFieldType dvFieldType = (DenseVectorFieldMapper.DenseVectorFieldType) fieldType;

        if (dvFieldType.isSearchable() == false) {
            throw new AggregationExecutionException(
                "field ["
                    + field
                    + "] must be an indexed dense_vector field for ["
                    + OutlierDetectionAggregationBuilder.NAME
                    + "] aggregation"
            );
        }

        DenseVectorFieldMapper.ElementType elementType = dvFieldType.getElementType();
        if (elementType != DenseVectorFieldMapper.ElementType.FLOAT && elementType != DenseVectorFieldMapper.ElementType.BFLOAT16) {
            throw new AggregationExecutionException(
                "field ["
                    + field
                    + "] must have element type float or bfloat16 for ["
                    + OutlierDetectionAggregationBuilder.NAME
                    + "] aggregation; got ["
                    + elementType
                    + "]"
            );
        }

        int dims = dvFieldType.getVectorDimensions();

        int effectiveProjectionDim = projectionDim != null ? projectionDim : Math.min(dims, 50);
        if (effectiveProjectionDim > dims) {
            effectiveProjectionDim = dims;
        }

        long effectiveSeed = seed != null ? seed : System.nanoTime();

        return new OutlierDetectionAggregator(
            name,
            context,
            parent,
            metadata,
            field,
            dvFieldType,
            dims,
            topN,
            nNeighbors,
            sampleSize,
            effectiveProjectionDim,
            effectiveSeed,
            overfetchFactor,
            method,
            maxCoordSample,
            normalize
        );
    }
}
