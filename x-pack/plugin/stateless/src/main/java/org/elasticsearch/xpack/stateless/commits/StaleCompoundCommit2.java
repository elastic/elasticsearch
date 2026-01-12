/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.index.shard.ShardId;

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
