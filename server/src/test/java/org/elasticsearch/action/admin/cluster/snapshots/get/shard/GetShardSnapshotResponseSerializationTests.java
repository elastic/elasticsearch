/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get.shard;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
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
        assertThat(newInstance.getRepositoryShardSnapshots(), equalTo(expectedInstance.getRepositoryShardSnapshots()));
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
            (out, value) -> value.writeTo(out),
            GetShardSnapshotResponse::new,
            Version.CURRENT
        );
    }

    private GetShardSnapshotResponse createTestInstance() {
        Map<String, ShardSnapshotInfo> repositoryShardSnapshots = randomMap(0, randomIntBetween(1, 10), this::repositoryShardSnapshot);
        Map<String, RepositoryException> repositoryFailures = randomMap(0, randomIntBetween(1, 10), this::repositoryFailure);

        return new GetShardSnapshotResponse(repositoryShardSnapshots, repositoryFailures);
    }

    private Tuple<String, ShardSnapshotInfo> repositoryShardSnapshot() {
        String repositoryName = randomString(50);

        ShardId shardId = new ShardId(randomString(50), UUIDs.randomBase64UUID(), randomIntBetween(0, 100));
        SnapshotInfo snapshotInfo = SnapshotInfoTestUtils.createRandomSnapshotInfo();
        String indexMetadataIdentifier = randomString(50);
        List<BlobStoreIndexShardSnapshot.FileInfo> indexFiles = randomList(1, 10, this::randomFileInfo);
        SnapshotFiles snapshotFiles = new SnapshotFiles(randomString(10), indexFiles, randomBoolean() ? randomString(20) : null);

        IndexId indexId = new IndexId(randomFrom(snapshotInfo.indices()), randomString(25));
        return Tuple.tuple(repositoryName, new ShardSnapshotInfo(indexId, shardId, snapshotInfo, indexMetadataIdentifier, snapshotFiles));
    }

    private BlobStoreIndexShardSnapshot.FileInfo randomFileInfo() {
        String name = randomString(25);

        final BytesRef hash;
        if (randomBoolean()) {
            hash = new BytesRef(scaledRandomIntBetween(0, 1024 * 1024));
            hash.length = hash.bytes.length;
            for (int i = 0; i < hash.length; i++) {
                hash.bytes[i] = randomByte();
            }
        } else {
            hash = null;
        }

        StoreFileMetadata meta = new StoreFileMetadata(
            name,
            randomLongBetween(0, new ByteSizeValue(4, ByteSizeUnit.GB).getBytes()),
            randomString(10),
            randomFrom(Version.CURRENT.luceneVersion, Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion).toString(),
            hash
        );

        ByteSizeValue partSize = new ByteSizeValue(randomLongBetween(1, 2048), randomFrom(ByteSizeUnit.KB, ByteSizeUnit.MB));

        return new BlobStoreIndexShardSnapshot.FileInfo(name, meta, partSize);
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
