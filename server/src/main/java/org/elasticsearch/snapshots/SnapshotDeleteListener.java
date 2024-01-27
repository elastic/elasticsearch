/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.repositories.RepositoryData;

public interface SnapshotDeleteListener {

    /**
     * Invoked once the snapshots have been fully deleted from the repository, including all async cleanup operations, indicating that
     * listeners waiting for the end of the deletion can now be notified.
     */
    void onDone();

    /**
     * Invoked once the updated {@link RepositoryData} has been written to the repository and it is safe for the next repository operation
     * to proceed.
     *
     * @param repositoryData updated repository data
     */
    void onRepositoryDataWritten(RepositoryData repositoryData);

    /**
     * Invoked if writing updated {@link RepositoryData} to the repository failed. Once {@link #onRepositoryDataWritten(RepositoryData)} has
     * been invoked this method will never be invoked.
     *
     * @param e exception during metadata steps of snapshot delete
     */
    void onFailure(Exception e);
}
