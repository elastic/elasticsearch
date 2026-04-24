/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

/**
 * Result of applying a pre-flight filter using only {@link RepositoryData}.
 * <p>
 * When the needed information is not available in {@link RepositoryData}, the filter cannot definitively include or exclude the snapshot;
 * that case is represented by {@link #INCONCLUSIVE}.
 */
enum PreflightFilterResult {
    /** The snapshot definitely passes the filter. */
    INCLUDE,

    /** The snapshot definitely fails the filter. */
    EXCLUDE,

    /** The result cannot be determined from {@link RepositoryData} alone; full {@link SnapshotInfo} will be needed. */
    INCONCLUSIVE;

    @FunctionalInterface
    interface RepositoryDataFilter {
        /**
         * @return an accurate result for {@link SnapshotSortKey#NAME}, {@link SnapshotSortKey#INDICES} and
         * {@link SnapshotSortKey#REPOSITORY} (the latter because {@code GetSnapshotsOperation#skipRepository} handles this case even
         * earlier). Also accurate for {@link SnapshotSortKey#START_TIME} and {@link SnapshotSortKey#DURATION} if and only if the
         * corresponding values are available in {@link RepositoryData.SnapshotDetails}; returns {@link PreflightFilterResult#INCONCLUSIVE}
         * otherwise.
         */
        PreflightFilterResult test(SnapshotId snapshotId, RepositoryData repositoryData);
    }
}
