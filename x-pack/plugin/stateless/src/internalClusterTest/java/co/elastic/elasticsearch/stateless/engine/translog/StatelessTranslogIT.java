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

package co.elastic.elasticsearch.stateless.engine.translog;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StatelessTranslogIT extends AbstractStatelessIntegTestCase {

    public void testTranslogFileHoldDirectoryOfReferencedFiles() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(secondActiveTranslogFiles.size(), greaterThan(firstFileCount));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
    }

    public void testTranslogFileHoldDirectoryForIdleShards() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        final String idleIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            idleIndex,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ShardId idleShardId = new ShardId(resolveIndex(idleIndex), 0);

        ensureGreen(indexName, idleIndex);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
            indexDocs(idleIndex, randomIntBetween(1, 20));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        long maxUploadedFileAfterFirstIndex = translogReplicator.getMaxUploadedFile();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(secondActiveTranslogFiles.size(), greaterThan(firstFileCount));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
        assertDirectoryConsistency(blobs, translogBlobContainer, idleShardId);
        BlobMetadata lastBlob = blobs.get(blobs.size() - 1);
        try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, lastBlob.name()))) {
            long generation = Long.parseLong(lastBlob.name());
            CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(lastBlob.name(), streamInput);
            TranslogMetadata metadata = header.metadata().get(idleShardId);
            long maxReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                .mapToLong(r -> generation - r)
                .max()
                .getAsLong();
            assertThat(maxReferenced, lessThanOrEqualTo(maxUploadedFileAfterFirstIndex));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1526")
    public void testTranslogFileHoldDirectoryReflectsWhenFilesPruned() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        flush(indexName);

        assertBusy(() -> assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0)));
        assertBusy(() -> assertTrue(translogBlobContainer.listBlobs(operationPurpose).isEmpty()));

        indexDocs(indexName, randomIntBetween(1, 20));
        long onlyReferencedFile = translogReplicator.getMaxUploadedFile();

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(translogReplicator.getMaxUploadedFile(), greaterThan(onlyReferencedFile));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
        BlobMetadata lastBlob = blobs.get(blobs.size() - 1);
        try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, lastBlob.name()))) {
            long generation = Long.parseLong(lastBlob.name());
            CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(lastBlob.name(), streamInput);
            TranslogMetadata metadata = header.metadata().get(shardId);
            long minReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                .mapToLong(r -> generation - r)
                .min()
                .getAsLong();
            assertThat(minReferenced, equalTo(onlyReferencedFile));
        }
    }

    private static void assertDirectoryConsistency(List<BlobMetadata> blobs, BlobContainer translogBlobContainer, ShardId shardId)
        throws IOException {
        long totalOps = 0;
        HashSet<Long> referencedFiles = new HashSet<>();
        for (BlobMetadata blob : blobs) {
            long generation = Long.parseLong(blob.name());
            try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, blob.name()))) {
                CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(blob.name(), streamInput);
                TranslogMetadata metadata = header.metadata().get(shardId);
                totalOps += metadata.totalOps();
                assertThat(metadata.directory().estimatedOperationsToRecover(), equalTo(totalOps));
                Set<Long> actualReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                    .mapToLong(r -> generation - r)
                    .boxed()
                    .collect(Collectors.toSet());

                assertThat(actualReferenced, equalTo(referencedFiles));
                if (metadata.totalOps() > 0) {
                    referencedFiles.add(generation);
                }
            }
        }
    }

    private static void assertTranslogBlobsExist(Set<TranslogReplicator.BlobTranslogFile> shouldExist, BlobContainer container)
        throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : shouldExist) {
            assertTrue(container.blobExists(operationPurpose, translogFile.blobName()));
        }
    }

}
