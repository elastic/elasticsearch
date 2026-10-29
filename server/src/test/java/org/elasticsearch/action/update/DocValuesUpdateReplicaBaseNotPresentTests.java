/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.DocValuesUpdateBaseNotPresentException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * A doc-values delta only applies against the base version it was computed against. On a copy that has not applied that base yet - a
 * still-recovering copy receiving live operations, or a CCR follower replica that got the delta batch before the base - the engine rejects
 * it with {@link DocValuesUpdateBaseNotPresentException} (see {@code InternalEngine#planDocValuesUpdateAsNonPrimary}).
 * {@code ReplicationOperation} fails the replica for any exception that is not {@link TransportActions#isShardNotAvailableException}, so on
 * the normal replication path the shard is failed and recovery restarts; only the CCR leader fetch ({@code ShardFollowNodeTask#shouldRetry})
 * retries it. The primary avoids shipping such a delta by gating the in-place fast path on every tracked copy holding the base (see
 * {@code UpdateHelper}); this covers the residual path where a delta still reaches a copy without the base.
 */
public class DocValuesUpdateReplicaBaseNotPresentTests extends IndexShardTestCase {

    private static final String MAPPING = """
        { "properties": { "count": { "type": "long", "index": false, "doc_values": { "updatable": true } } } }
        """;

    public void testBaseNotPresentDeltaFailsShardInsteadOfRetryingOnReplicationPath() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());

        final ShardId shardId = new ShardId("index", "_na_", 0);
        final Settings settings = indexSettings(IndexVersion.current(), 1, 1).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            // updatable fields need sequence numbers, which columnar disables by default
            .put("index.disable_sequence_numbers", false)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings)
            .putMapping(MAPPING)
            .primaryTerm(0, 1)
            .build();

        final IndexShard primary = newShard(shardId, true, "primary-node", indexMetadata, null);
        IndexShard replica = null;
        try {
            recoverShardFromStore(primary);
            replica = newShard(shardId, false, "replica-node", indexMetadata, null);
            // Recover an empty replica. It has not received the base operation (sequence number 0): this stands in for a still-recovering
            // copy whose phase-2 replay has not reached the base yet, or a CCR follower replica that received the delta batch first.
            recoverReplica(replica, primary, true);

            final long baseSeqNo = 0L;
            final long deltaSeqNo = 1L;
            final IndexShard replicaRef = replica;

            // The delta (sequence number 1) was computed against the base document at sequence number 0, which this replica does not hold.
            final DocValuesUpdateBaseNotPresentException e = expectThrows(
                DocValuesUpdateBaseNotPresentException.class,
                () -> replicaRef.applyDocValuesUpdateOnReplica(
                    deltaSeqNo,
                    1L,
                    1L,
                    "1",
                    List.of(new Translog.DocValuesUpdate.NumericFieldUpdate("count", 42)),
                    baseSeqNo,
                    1L
                )
            );

            // ReplicationOperation only skips failing the replica for shard-unavailable exceptions; this one is not, so it fails the shard
            // and recovery restarts rather than the operation being retried once the base arrives, which is what the CCR fetch path does.
            assertThat(
                "base-not-present is treated as a shard failure on the replication path, so recovery restarts rather than retrying the op",
                TransportActions.isShardNotAvailableException(e),
                equalTo(false)
            );
        } finally {
            closeShards(primary, replica);
        }
    }
}
