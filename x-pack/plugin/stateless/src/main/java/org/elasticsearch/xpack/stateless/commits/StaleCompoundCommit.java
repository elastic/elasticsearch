/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

public record StaleCompoundCommit(ShardId shardId, PrimaryTermAndGeneration primaryTermAndGeneration, long allocationPrimaryTerm) {
    public long primaryTerm() {
        return primaryTermAndGeneration.primaryTerm();
    }

    public String fileName() {
        return StatelessCompoundCommit.blobNameFromGeneration(primaryTermAndGeneration.generation());
    }

    public String absoluteBlobPath(BlobPath blobPath) {
        return blobPath.buildAsString() + fileName();
    }
}
