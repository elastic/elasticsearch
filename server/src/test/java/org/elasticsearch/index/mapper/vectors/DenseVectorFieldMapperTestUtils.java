/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.vectors.diskbbq.es94.ES940DiskBBQVectorsFormat;
import org.elasticsearch.inference.SimilarityMeasure;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BBQ_DIMS_DEFAULT_THRESHOLD;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BFLOAT16_DEFAULT_INDEX_OPTIONS;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BFLOAT16_DEFAULT_INDEX_OPTIONS_BACKPORT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DEFAULT_OVERSAMPLE;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ES_VERSION_94;

public class DenseVectorFieldMapperTestUtils {
    private DenseVectorFieldMapperTestUtils() {}

    public static List<SimilarityMeasure> getSupportedSimilarities(DenseVectorFieldMapper.ElementType elementType) {
        return switch (elementType) {
            case FLOAT, BFLOAT16, BYTE -> List.of(SimilarityMeasure.values());
            case BIT -> List.of(SimilarityMeasure.L2_NORM);
        };
    }

    public static int getEmbeddingLength(DenseVectorFieldMapper.ElementType elementType, int dimensions) {
        return switch (elementType) {
            case FLOAT, BFLOAT16, BYTE -> dimensions;
            case BIT -> {
                assert dimensions % Byte.SIZE == 0;
                yield dimensions / Byte.SIZE;
            }
        };
    }

    public static int randomCompatibleDimensions(DenseVectorFieldMapper.ElementType elementType, int max) {
        if (max < 1) {
            throw new IllegalArgumentException("max must be at least 1");
        }

        return switch (elementType) {
            case FLOAT, BFLOAT16, BYTE -> RandomNumbers.randomIntBetween(random(), 1, max);
            case BIT -> {
                if (max < 8) {
                    throw new IllegalArgumentException("max must be at least 8 for bit vectors");
                }

                // Generate a random dimension count that is a multiple of 8
                int maxEmbeddingLength = max / 8;
                yield RandomNumbers.randomIntBetween(random(), 1, maxEmbeddingLength) * 8;
            }
        };
    }

    public static Set<DenseVectorFieldMapper.ElementType> elementTypesWithDefaultIndexOptions(IndexVersion indexVersion) {
        Set<DenseVectorFieldMapper.ElementType> elementTypes = new HashSet<>();
        elementTypes.add(DenseVectorFieldMapper.ElementType.FLOAT);
        if (indexVersion.onOrAfter(BFLOAT16_DEFAULT_INDEX_OPTIONS)
            || indexVersion.between(BFLOAT16_DEFAULT_INDEX_OPTIONS_BACKPORT, ES_VERSION_94)) {
            elementTypes.add(DenseVectorFieldMapper.ElementType.BFLOAT16);
        }

        return Collections.unmodifiableSet(elementTypes);
    }

    private static Random random() {
        return RandomizedContext.current().getRandom();
    }

    /**
     * This method replicates the logic used in DenseVectorFieldMapper.Builder::defaultIndexOptions as this is the best and easiest way
     * to get expected default index options.
     */
    public static DenseVectorFieldMapper.DenseVectorIndexOptions defaultDenseVectorIndexOptions(
        IndexVersion indexVersionCreated,
        boolean enterpriseLicense,
        Integer dims,
        DenseVectorFieldMapper.ElementType elementType,
        boolean experimentalFeaturesEnabled
    ) {
        if (elementTypesWithDefaultIndexOptions(indexVersionCreated).contains(elementType) == false) {
            return null;
        }

        // These are the default index options for dense_vector fields, used then semantic_text does not default to bbq_disk.
        final boolean defaultInt8Hnsw = indexVersionCreated.onOrAfter(IndexVersions.DEFAULT_DENSE_VECTOR_TO_INT8_HNSW);
        final boolean defaultBBQHnsw = indexVersionCreated.onOrAfter(IndexVersions.DEFAULT_DENSE_VECTOR_TO_BBQ_HNSW);
        final boolean defaultBBQDisk = indexVersionCreated.onOrAfter(IndexVersions.DEFAULT_DENSE_VECTOR_TO_BBQ_DISK);

        if (defaultBBQDisk && enterpriseLicense) {
            int bits = dims < BBQ_DIMS_DEFAULT_THRESHOLD ? 4 : 1;
            return new DenseVectorFieldMapper.BBQIVFIndexOptions(
                ES940DiskBBQVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER,
                -1,
                0d,
                false,
                new DenseVectorFieldMapper.RescoreVector(DEFAULT_OVERSAMPLE),
                indexVersionCreated,
                false,
                bits,
                experimentalFeaturesEnabled
            );
        }

        if (defaultBBQHnsw && dims >= BBQ_DIMS_DEFAULT_THRESHOLD) {
            return new DenseVectorFieldMapper.BBQHnswIndexOptions(
                Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
                Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                false,
                new DenseVectorFieldMapper.RescoreVector(DEFAULT_OVERSAMPLE),
                -1
            );
        }
        if (defaultInt8Hnsw) {
            return new DenseVectorFieldMapper.Int8HnswIndexOptions(
                Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
                Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                false,
                null,
                -1
            );
        }

        return null;
    }
}
