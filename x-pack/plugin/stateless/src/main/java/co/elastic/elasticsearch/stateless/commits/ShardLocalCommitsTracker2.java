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

/**
 * Encapsulates shard-level tracking of acquired commits and open readers to prevent
 * premature deletion of referenced blobs during engine operations.
 *
 * <p>
 *  This tracker maintains state that persists across {@link org.elasticsearch.index.shard.IndexShard#resetEngine}
 *  operations, ensuring that commit references and reader state are preserved to avoid
 *  deleting blobs that are still in use by active readers or referenced commits.
 * </p>
 */
public record ShardLocalCommitsTracker(ShardLocalReadersTracker shardLocalReadersTracker, ShardLocalCommitsRefs shardLocalCommitsRefs) {}
