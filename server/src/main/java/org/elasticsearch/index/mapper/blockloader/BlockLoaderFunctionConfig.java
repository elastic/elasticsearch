/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Objects;

/**
 * Configuration needed to transform loaded values into blocks.
 * {@link MappedFieldType}s will find me in
 * {@link MappedFieldType.BlockLoaderContext#blockLoaderFunctionConfig()} and
 * use this configuration to choose the appropriate implementation for
 * transforming loaded values into blocks.
 */
public interface BlockLoaderFunctionConfig {
    /**
     * Name used in descriptions.
     */
    Function function();

    record JustWarnings(Function function, Warnings warnings) implements BlockLoaderFunctionConfig {

        // Consider just the function, as warnings will have Source that differ for different invocations of the same function
        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            JustWarnings that = (JustWarnings) o;
            return function == that.function;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(function);
        }
    }

    enum Function {
        LENGTH,
        V_COSINE,
        V_DOT_PRODUCT,
        V_HAMMING,
        V_L1NORM,
        V_L2NORM,
    }
}
