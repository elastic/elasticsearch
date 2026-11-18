/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.plugins.internal;

import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.RestoreService;

/**
 * Maintains one {@link Metadata.ProjectCustom} of a project as its restores are initialized and completed, in the same cluster-state
 * updates that install and remove the {@link RestoreInProgress} entry, so that what it records is published atomically with them.
 * <p>
 * {@link RestoreService} calls this while it runs those updates, before they are known to be committed. So an implementation computes the
 * new custom from its arguments and returns it, and does nothing else: no I/O, no notifications, and no state kept anywhere else, because
 * an update that is not committed, for example after a master failover, is run again. Returning the custom, rather than a whole cluster
 * state, also leaves it to {@link RestoreService} to put it into the state that it is already building.
 * <p>
 * An implementation maintains a single type of custom, {@link #projectCustomType()}, and only for the project that the restore belongs to.
 * It never removes the custom: to leave nothing, return an empty one.
 */
public interface RestoreProjectCustomUpdater {

    /**
     * The type of the {@link Metadata.ProjectCustom} that this maintains, which is the name it is put into the project under, or
     * {@code null} if there is none, in which case nothing else is called.
     */
    @Nullable
    default String projectCustomType() {
        return null;
    }

    /**
     * Called within the cluster-state update that installs a new {@link RestoreInProgress} entry.
     * <p>
     * This is not given the project. The update has not finished changing it, and only {@code current} is needed to record that a restore
     * was installed. A restore is rejected, and nothing is installed, if this throws.
     *
     * @param entry   the newly installed restore entry
     * @param current the project's custom of type {@link #projectCustomType()}, or {@code null} if it has none yet
     * @return the custom to publish in its place, or {@code null} to leave the project's as it is
     */
    @Nullable
    default Metadata.ProjectCustom onRestoreInitialized(RestoreInProgress.Entry entry, @Nullable Metadata.ProjectCustom current) {
        return null;
    }

    /**
     * Called within the cluster-state update that removes a completed {@link RestoreInProgress} entry.
     *
     * <p>This method may be called more than once for the same entry: if a previous invocation threw
     * an exception, the entry is retained in {@link RestoreInProgress} and this method will be called
     * again on the next cleanup pass. Implementations must be idempotent with respect to repeated
     * calls for the same entry.
     *
     * @param entry   the completed restore entry, which is removed by this update; its {@link RestoreInProgress.Entry#state()} is terminal
     * @param project the entry's project as it was when this cleanup started. Customs that earlier entries of the same cleanup changed
     *                are in {@code current}, not here
     * @param current the project's custom of type {@link #projectCustomType()}, with the changes that earlier entries of the same cleanup
     *                made, or {@code null} if it has none
     * @return the custom to publish in its place, or {@code null} to leave the project's as it is
     */
    @Nullable
    default Metadata.ProjectCustom onRestoreCompleted(
        RestoreInProgress.Entry entry,
        ProjectMetadata project,
        @Nullable Metadata.ProjectCustom current
    ) {
        return null;
    }

    /** Maintains nothing. Used when no implementation is registered. */
    RestoreProjectCustomUpdater NOOP = new RestoreProjectCustomUpdater() {};
}
