/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get.shard;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GetShardSnapshotResponseSerializationTests extends ESTestCase {

    public void testSerialization() throws IOException {
        // We don't use AbstractWireSerializingTestCase here since it is based on equals and hashCode and
        // GetShardSnapshotResponse contains RepositoryException instances that don't implement these methods.
        GetShardSnapshotResponse testInstance = createTestInstance();
        GetShardSnapshotResponse deserializedInstance = copyInstance(testInstance);
        assertEqualInstances(testInstance, deserializedInstance);
    }

    private void assertEqualInstances(GetShardSnapshotResponse expectedInstance, GetShardSnapshotResponse newInstance) {
        assertThat(newInstance.getLatestShardSnapshot(), equalTo(expectedInstance.getLatestShardSnapshot()));
        assertEquals(expectedInstance.getRepositoryFailures().keySet(), newInstance.getRepositoryFailures().keySet());
        for (Map.Entry<String, RepositoryException> expectedEntry : expectedInstance.getRepositoryFailures().entrySet()) {
            ElasticsearchException expectedException = expectedEntry.getValue();
            ElasticsearchException newException = newInstance.getRepositoryFailures().get(expectedEntry.getKey());
            assertThat(newException.getMessage(), containsString(expectedException.getMessage()));
        }
    }

    private GetShardSnapshotResponse copyInstance(GetShardSnapshotResponse instance) throws IOException {
        return copyInstance(
            instance,
            new NamedWriteableRegistry(Collections.emptyList()),
            StreamOutput::writeWriteable,
            GetShardSnapshotResponse::new,
            TransportVersion.current()
        );
    }

    private GetShardSnapshotResponse createTestInstance() {
        ShardSnapshotInfo latestSnapshot = repositoryShardSnapshot();
        Map<String, RepositoryException> repositoryFailures = randomMap(0, randomIntBetween(1, 10), this::repositoryFailure);

        return new GetShardSnapshotResponse(latestSnapshot, repositoryFailures);
    }

    private ShardSnapshotInfo repositoryShardSnapshot() {
        final String indexName = randomString(50);
        ShardId shardId = new ShardId(indexName, UUIDs.randomBase64UUID(), randomIntBetween(0, 100));
        Snapshot snapshot = new Snapshot(randomAlphaOfLength(5), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        String indexMetadataIdentifier = randomString(50);

        IndexId indexId = new IndexId(indexName, randomString(25));
        String shardStateIdentifier = randomBoolean() ? randomString(30) : null;
        return new ShardSnapshotInfo(indexId, shardId, snapshot, indexMetadataIdentifier, shardStateIdentifier, randomLongBetween(0, 2048));
    }

    private Tuple<String, RepositoryException> repositoryFailure() {
        String repositoryName = randomString(25);
        Throwable cause = randomBoolean() ? null : randomException();
        RepositoryException repositoryException = new RepositoryException(repositoryName, randomString(1024), cause);
        return Tuple.tuple(repositoryName, repositoryException);
    }

    private Exception randomException() {
        return randomFrom(
            new FileNotFoundException(),
            new IOException(randomString(15)),
            new IllegalStateException(randomString(10)),
            new IllegalArgumentException(randomString(20)),
            new EsRejectedExecutionException(randomString(10))
        );
    }

    private String randomString(int maxLength) {
        return randomAlphaOfLength(randomIntBetween(1, maxLength));
    }
}
