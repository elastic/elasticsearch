/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.IndexMetadata;

/**
 * A temporary interface meant to allow a plugin to provide remedial logic for index metadata being restored from a snapshot. This was
 * added to address an allocation issue and will eventually be removed once any affected snapshots age out.
 */
public interface IndexMetadataRestoreTransformer {
    IndexMetadata updateIndexMetadata(IndexMetadata original);

    /**
     * A default implementation of {@link IndexMetadataRestoreTransformer} which does nothing
     */
    final class NoOpRestoreTransformer implements IndexMetadataRestoreTransformer {
        public static final NoOpRestoreTransformer INSTANCE = new NoOpRestoreTransformer();

        public static NoOpRestoreTransformer getInstance() {
            return INSTANCE;
        }

        private NoOpRestoreTransformer() {}

        @Override
        public IndexMetadata updateIndexMetadata(IndexMetadata original) {
            return original;
        }
    }
}
