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

/**
 * Configuration needed to transform loaded values into blocks.
 * {@link MappedFieldType}s will find me in
 * {@link MappedFieldType.BlockLoaderContext#blockLoaderFunctionConfig()} and
 * use this configuration to choose the appropriate implementation for
 * transforming loaded values into blocks.
 */
public interface BlockLoaderFunctionConfig {
    record Named(String name, Warnings warnings) implements BlockLoaderFunctionConfig {
        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Named named = (Named) o;
            return name.equals(named.name);
        }
    }
}
