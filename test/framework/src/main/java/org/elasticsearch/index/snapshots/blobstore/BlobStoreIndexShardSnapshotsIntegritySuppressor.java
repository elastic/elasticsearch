/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.elasticsearch.core.Releasable;

/**
 * Test utility class to suppress assertions about the integrity of the contents of a blobstore repository, in order to verify the
 * production behaviour on encountering invalid data.
 */
public class BlobStoreIndexShardSnapshotsIntegritySuppressor implements Releasable {

    public BlobStoreIndexShardSnapshotsIntegritySuppressor() {
        BlobStoreIndexShardSnapshots.INTEGRITY_ASSERTIONS_ENABLED = false;
    }

    @Override
    public void close() {
        BlobStoreIndexShardSnapshots.INTEGRITY_ASSERTIONS_ENABLED = true;
    }
}
