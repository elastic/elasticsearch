/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSIvfPqIndexParams;
import com.nvidia.cuvs.CuVSIvfPqParams;
import com.nvidia.cuvs.CuVSIvfPqSearchParams;
import com.nvidia.cuvs.CuVSMatrix;

/**
 * Factory for creating {@link CuVSIvfPqParams} with automatic parameter calculation
 * based on dataset dimensions and distance metric.
 *
 * <p>This class provides a Java equivalent to the C++ function:
 * {@code cuvs::neighbors::graph_build_params::ivf_pq_params(dataset_extents, metric)}
 *
 * <p>The parameters are calculated automatically based on the dataset shape (number of
 * rows and features), following the same heuristics as the C++ implementation.
 *
 * TODO: Remove this class when cuvs 25.12 is available and use functions from there directly.
 */
public class CuVSIvfPqParamsFactory {

    /**
     * Creates {@link CuVSIvfPqParams} with automatically calculated parameters based on the
     * dataset dimensions and distance metric.
     *
     * <p>This method replicates the parameter calculation logic from the C++ function:
     * {@code cuvs::neighbors::graph_build_params::ivf_pq_params(dataset_extents, metric)}
     */
    public static CuVSIvfPqParams create(CuVSMatrix dataset, CagraIndexParams.CuvsDistanceType distanceType) {
        if (dataset == null) {
            throw new IllegalArgumentException("dataset cannot be null");
        }

        long nRows = dataset.size();
        long nFeatures = dataset.columns();

        if (nRows <= 0 || nFeatures <= 0) {
            throw new IllegalArgumentException("Dataset dimensions must be positive: rows=" + nRows + ", features=" + nFeatures);
        }
        return createFromDimensions(nRows, nFeatures, distanceType);
    }

    /**
     * Creates {@link CuVSIvfPqParams} with automatically calculated parameters based on dataset
     * dimensions.
     *
     * <p>This is a convenience method when you have the dataset dimensions but not the dataset
     * object itself. The calculation logic is identical to {@link #create(CuVSMatrix,
     * CagraIndexParams.CuvsDistanceType)}.
     *
     * @param nRows the number of rows (vectors) in the dataset
     * @param nFeatures the number of features (dimensions) per vector
     * @param distanceType the distance metric to use (e.g., L2Expanded, Cosine)
     * @return a {@link CuVSIvfPqParams} instance with calculated parameters
     * @throws IllegalArgumentException if dimensions are invalid
     */
    public static CuVSIvfPqParams createFromDimensions(long nRows, long nFeatures, CagraIndexParams.CuvsDistanceType distanceType) {
        if (nRows <= 0 || nFeatures <= 0) {
            throw new IllegalArgumentException("Dataset dimensions must be positive: rows=" + nRows + ", features=" + nFeatures);
        }

        // Calculate PQ dimensions and bits based on feature count
        int pqDim;
        int pqBits;

        if (nFeatures <= 32) {
            pqDim = 16;
            pqBits = 8;
        } else {
            pqBits = 4;
            if (nFeatures <= 64) {
                pqDim = 32;
            } else if (nFeatures <= 128) {
                pqDim = 64;
            } else if (nFeatures <= 192) {
                pqDim = 96;
            } else {
                // Round up to nearest multiple of 128
                pqDim = (int) roundUpSafe(nFeatures / 2, 128);
            }
        }
        // Calculate number of lists: approximately 1 cluster per 2000 rows
        int nLists = Math.max(1, (int) (nRows / 2000));

        // Calculate kmeans training set fraction adaptively
        final double kMinPointsPerCluster = 32.0;
        final double minKmeansTrainsetPoints = kMinPointsPerCluster * nLists;
        final double maxKmeansTrainsetFraction = 1.0;
        final double minKmeansTrainsetFraction = Math.min(maxKmeansTrainsetFraction, minKmeansTrainsetPoints / nRows);
        double kmeansTrainsetFraction = Math.clamp(1.0 / Math.sqrt(nRows * 1e-5), minKmeansTrainsetFraction, maxKmeansTrainsetFraction);

        // Calculate number of probes based on number of lists
        int nProbes = Math.round((float) (Math.sqrt(nLists) / 20.0 + 4.0));

        // Build index parameters
        CuVSIvfPqIndexParams indexParams = new CuVSIvfPqIndexParams.Builder().withMetric(distanceType)
            .withPqDim(pqDim)
            .withPqBits(pqBits)
            .withNLists(nLists)
            .withKmeansNIters(10)
            .withKmeansTrainsetFraction(kmeansTrainsetFraction)
            .withCodebookKind(CagraIndexParams.CodebookGen.PER_SUBSPACE)
            .build();

        // Build search parameters
        CuVSIvfPqSearchParams searchParams = new CuVSIvfPqSearchParams.Builder().withNProbes(nProbes)
            .withLutDtype(CagraIndexParams.CudaDataType.CUDA_R_16F)
            .withInternalDistanceDtype(CagraIndexParams.CudaDataType.CUDA_R_16F)
            .build();

        // Build and return the complete IVF_PQ parameters
        return new CuVSIvfPqParams.Builder().withCuVSIvfPqIndexParams(indexParams)
            .withCuVSIvfPqSearchParams(searchParams)
            .withRefinementRate(1.0f)
            .build();
    }

    /**
     * Helper method to round up to the nearest multiple of a given divisor.
     *
     * <p>Equivalent to C++ {@code raft::round_up_safe<uint32_t>(value, divisor)}
     *
     * @param value the value to round up
     * @param divisor the divisor to round to
     * @return the rounded up value
     */
    private static long roundUpSafe(long value, long divisor) {
        if (divisor <= 0) {
            throw new IllegalArgumentException("divisor must be positive");
        }
        return ((value + divisor - 1) / divisor) * divisor;
    }
}
