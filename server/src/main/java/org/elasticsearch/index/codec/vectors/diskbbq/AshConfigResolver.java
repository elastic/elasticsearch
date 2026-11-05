/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.core.Nullable;

/**
 * Supplies the {@link IvfSegmentConfig.AshConfig} an ASH writer uses for a field. Some ASH parameters depend on the
 * vector dimension, which the mapping may not know until the first document arrives, so the writer resolves the
 * configuration per field from the dimension it is actually writing.
 */
public sealed interface AshConfigResolver {

    /** Returns the configuration to write a field with {@code dims} dimensions. */
    IvfSegmentConfig.AshConfig resolve(int dims);

    /** Query quantization bits, which do not depend on the dimension. */
    int queryBitsPerDim();

    /** Always uses the given configuration, whatever the dimension. */
    static AshConfigResolver fixed(IvfSegmentConfig.AshConfig config) {
        return new Fixed(config);
    }

    /**
     * Derives the configuration from the dimension with {@link IvfSegmentConfig.AshConfig#forDims}.
     *
     * @param bitsPerDim the bits per projected dimension the user configured, or {@code null} to derive them
     */
    static AshConfigResolver dimsDerived(@Nullable Integer bitsPerDim, int queryBitsPerDim) {
        return new DimsDerived(bitsPerDim, queryBitsPerDim);
    }

    record Fixed(IvfSegmentConfig.AshConfig config) implements AshConfigResolver {
        @Override
        public IvfSegmentConfig.AshConfig resolve(int dims) {
            return config;
        }

        @Override
        public int queryBitsPerDim() {
            return config.queryBitsPerDim();
        }
    }

    record DimsDerived(@Nullable Integer bitsPerDim, int queryBitsPerDim) implements AshConfigResolver {
        public DimsDerived {
            if (bitsPerDim != null) {
                IvfSegmentConfig.AshConfig.validateBitsPerDim(bitsPerDim);
            }
        }

        @Override
        public IvfSegmentConfig.AshConfig resolve(int dims) {
            return IvfSegmentConfig.AshConfig.forDims(dims, bitsPerDim, queryBitsPerDim);
        }
    }
}
