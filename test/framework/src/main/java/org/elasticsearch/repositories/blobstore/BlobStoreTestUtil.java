/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.Locale;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public final class BlobStoreTestUtil {

    public static void assertRepoConsistency(InternalTestCluster testCluster, String repoName) {
        final BlobStoreRepository repo =
            (BlobStoreRepository) testCluster.getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
        BlobStoreTestUtil.assertConsistency(repo, repo.threadPool().executor(ThreadPool.Names.GENERIC));
    }

    public static boolean blobExists(BlobContainer container, String blobName) throws IOException {
        try (InputStream ignored = container.readBlob(blobName)) {
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    /**
     * Assert that there are no unreferenced indices or unreferenced root-level metadata blobs in any repository.
     * TODO: Expand the logic here to also check for unreferenced segment blobs and shard level metadata
     * @param repository BlobStoreRepository to check
     * @param executor Executor to run all repository calls on. This is needed since the production {@link BlobStoreRepository}
     *                 implementations assert that all IO inducing calls happen on the generic or snapshot thread-pools and hence callers
     *                 of this assertion must pass an executor on those when using such an implementation.
     */
    public static void assertConsistency(BlobStoreRepository repository, Executor executor) {
        final PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
        executor.execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() throws Exception {
                final BlobContainer blobContainer = repository.blobContainer();
                final long latestGen;
                try (DataInputStream inputStream = new DataInputStream(blobContainer.readBlob("index.latest"))) {
                    latestGen = inputStream.readLong();
                } catch (NoSuchFileException e) {
                    throw new AssertionError("Could not find index.latest blob for repo [" + repository + "]");
                }
                assertIndexGenerations(blobContainer, latestGen);
                final RepositoryData repositoryData;
                try (InputStream blob = blobContainer.readBlob("index-" + latestGen);
                     XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                         LoggingDeprecationHandler.INSTANCE, blob)) {
                    repositoryData = RepositoryData.snapshotsFromXContent(parser, latestGen);
                }
                assertIndexUUIDs(blobContainer, repositoryData);
                assertSnapshotUUIDs(repository, repositoryData);
                listener.onResponse(null);
            }
        });
        listener.actionGet(TimeValue.timeValueMinutes(1L));
    }

    private static void assertIndexGenerations(BlobContainer repoRoot, long latestGen) throws IOException {
        final long[] indexGenerations = repoRoot.listBlobsByPrefix("index-").keySet().stream()
            .map(s -> s.replace("index-", ""))
            .mapToLong(Long::parseLong).sorted().toArray();
        assertEquals(latestGen, indexGenerations[indexGenerations.length - 1]);
        assertTrue(indexGenerations.length <= 2);
    }

    private static void assertIndexUUIDs(BlobContainer repoRoot, RepositoryData repositoryData) throws IOException {
        final List<String> expectedIndexUUIDs =
            repositoryData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toList());
        final BlobContainer indicesContainer = repoRoot.children().get("indices");
        final List<String> foundIndexUUIDs;
        if (indicesContainer == null) {
            foundIndexUUIDs = Collections.emptyList();
        } else {
            // Skip Lucene MockFS extraN directory
            foundIndexUUIDs = indicesContainer.children().keySet().stream().filter(
                s -> s.startsWith("extra") == false).collect(Collectors.toList());
        }
        assertThat(foundIndexUUIDs, containsInAnyOrder(expectedIndexUUIDs.toArray(Strings.EMPTY_ARRAY)));
    }

    private static void assertSnapshotUUIDs(BlobStoreRepository repository, RepositoryData repositoryData) throws IOException {
        final BlobContainer repoRoot = repository.blobContainer();
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        final List<String> expectedSnapshotUUIDs = snapshotIds.stream().map(SnapshotId::getUUID).collect(Collectors.toList());
        for (String prefix : new String[]{"snap-", "meta-"}) {
                final Collection<String> foundSnapshotUUIDs = repoRoot.listBlobs().keySet().stream().filter(p -> p.startsWith(prefix))
                    .map(p -> p.replace(prefix, "").replace(".dat", ""))
                    .collect(Collectors.toSet());
                assertThat(foundSnapshotUUIDs, containsInAnyOrder(expectedSnapshotUUIDs.toArray(Strings.EMPTY_ARRAY)));
        }

        final BlobContainer indicesContainer = repository.getBlobContainer().children().get("indices");
        final Map<String, BlobContainer> indices;
        if (indicesContainer == null) {
            indices = Collections.emptyMap();
        } else {
            indices = indicesContainer.children();
        }
        // Assert that for each snapshot, the relevant metadata was written to index and shard folders
        for (SnapshotId snapshotId: snapshotIds) {
            final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
            for (String index : snapshotInfo.indices()) {
                final IndexId indexId = repositoryData.resolveIndexId(index);
                assertThat(indices, hasKey(indexId.getId()));
                final BlobContainer indexContainer = indices.get(indexId.getId());
                assertThat(indexContainer.listBlobs(),
                    hasKey(String.format(Locale.ROOT, BlobStoreRepository.METADATA_NAME_FORMAT, snapshotId.getUUID())));
                for (Map.Entry<String, BlobContainer> entry : indexContainer.children().entrySet()) {
                    // Skip Lucene MockFS extraN directory
                    if (entry.getKey().startsWith("extra")) {
                        continue;
                    }
                    if (snapshotInfo.shardFailures().stream().noneMatch(shardFailure ->
                        shardFailure.index().equals(index) != false && shardFailure.shardId() == Integer.parseInt(entry.getKey()))) {
                        assertThat(entry.getValue().listBlobs(),
                            hasKey(String.format(Locale.ROOT, BlobStoreRepository.SNAPSHOT_NAME_FORMAT, snapshotId.getUUID())));
                    }
                }
            }
        }
    }

    public static long createDanglingIndex(BlobStoreRepository repository, String name, Set<String> files)
            throws InterruptedException, ExecutionException {
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        final AtomicLong totalSize = new AtomicLong();
        repository.threadPool().generic().execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repository.blobStore();
                BlobContainer container =
                    blobStore.blobContainer(repository.basePath().add("indices").add(name));
                for (String file : files) {
                    int size = randomIntBetween(0, 10);
                    totalSize.addAndGet(size);
                    container.writeBlob(file, new ByteArrayInputStream(new byte[size]), size, false);
                }
                future.onResponse(null);
            }
        });
        future.get();
        return totalSize.get();
    }

    public static void assertCorruptionVisible(BlobStoreRepository repository, Map<String, Set<String>> indexToFiles) {
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        repository.threadPool().generic().execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repository.blobStore();
                for (String index : indexToFiles.keySet()) {
                    if (blobStore.blobContainer(repository.basePath().add("indices"))
                        .children().containsKey(index) == false) {
                        future.onResponse(false);
                        return;
                    }
                    for (String file : indexToFiles.get(index)) {
                        try (InputStream ignored =
                                     blobStore.blobContainer(repository.basePath().add("indices").add(index)).readBlob(file)) {
                        } catch (NoSuchFileException e) {
                            future.onResponse(false);
                            return;
                        }
                    }
                }
                future.onResponse(true);
            }
        });
        assertTrue(future.actionGet());
    }

    public static void assertBlobsByPrefix(BlobStoreRepository repository, BlobPath path, String prefix, Map<String, BlobMetaData> blobs) {
        final PlainActionFuture<Map<String, BlobMetaData>> future = PlainActionFuture.newFuture();
        repository.threadPool().generic().execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repository.blobStore();
                future.onResponse(blobStore.blobContainer(path).listBlobsByPrefix(prefix));
            }
        });
        Map<String, BlobMetaData> foundBlobs = future.actionGet();
        if (blobs.isEmpty()) {
            assertThat(foundBlobs.keySet(), empty());
        } else {
            assertThat(foundBlobs.keySet(), containsInAnyOrder(blobs.keySet().toArray(Strings.EMPTY_ARRAY)));
            for (Map.Entry<String, BlobMetaData> entry : foundBlobs.entrySet()) {
                assertEquals(entry.getValue().length(), blobs.get(entry.getKey()).length());
            }
        }
    }
}
