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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BlobStoreTestUtil {

    private final BlobStoreRepository repository;
    private final ExecutorService executor;

    public BlobStoreTestUtil(BlobStoreRepository repository) {
        this.repository = repository;
        this.executor = repository.threadPool().executor(ThreadPool.Names.GENERIC);
    }

    public void deleteAndAssertEmpty(BlobPath path) throws Exception {
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        executor.execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                repository.blobStore().blobContainer(path).delete();
                future.onResponse(null);
            }
        });
        future.actionGet();
        final BlobPath parent = path.parent();
        if (parent == null) {
            assertChildren(path, Collections.emptyList());
        } else {
            assertDeleted(parent, path.toArray()[path.toArray().length - 1]);
        }
    }

    protected void assertDeleted(BlobPath path, String name) throws Exception {
        assertThat(listChildren(path), not(contains(name)));
    }

    public void assertChildren(BlobPath path, Collection<String> children) throws Exception {
        final Set<String> foundChildren = listChildren(path);
        if (children.isEmpty()) {
            assertThat(foundChildren, empty());
        } else {
            assertThat(foundChildren, containsInAnyOrder(children.toArray(Strings.EMPTY_ARRAY)));
        }
    }

    private Set<String> listChildren(BlobPath path) {
        final PlainActionFuture<Set<String>> future = PlainActionFuture.newFuture();
        executor.execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repository.blobStore();
                future.onResponse(blobStore.blobContainer(path).children().keySet());
            }
        });
        return future.actionGet();
    }

    /**
     * Assert that there are no unreferenced indices or unreferenced root-level metadata blobs in any repository.
     * TODO: Expand the logic here to also check for unreferenced segment blobs and shard level metadata
     */
    public void assertConsistency() throws Exception {
        final PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
        executor.execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() throws Exception {
                final BlobContainer blobContainer = repository.blobContainer();
                assertTrue(
                    "Could not find index.latest blob for repo [" + repository + "]", blobContainer.blobExists("index.latest"));
                final long latestGen;
                try (DataInputStream inputStream = new DataInputStream(blobContainer.readBlob("index.latest"))) {
                    latestGen = inputStream.readLong();
                }
                assertIndexGenerations(blobContainer, latestGen);
                final RepositoryData repositoryData;
                try (InputStream inputStream = blobContainer.readBlob("index-" + latestGen);
                     BytesStreamOutput out = new BytesStreamOutput()) {
                    Streams.copy(inputStream, out);
                    try (XContentParser parser =
                             XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                                 out.bytes(), XContentType.JSON)) {
                        repositoryData = RepositoryData.snapshotsFromXContent(parser, latestGen);
                    }
                }
                assertIndexUUIDs(blobContainer, repositoryData);
                assertSnapshotUUIDs(blobContainer, repositoryData);
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
            foundIndexUUIDs = indicesContainer.children().keySet().stream().filter(
                s -> s.startsWith("extra") == false).collect(Collectors.toList());
        }
        assertThat(foundIndexUUIDs, containsInAnyOrder(expectedIndexUUIDs.toArray(Strings.EMPTY_ARRAY)));
    }

    private static void assertSnapshotUUIDs(BlobContainer repoRoot, RepositoryData repositoryData) throws IOException {
        final List<String> expectedSnapshotUUIDs =
            repositoryData.getSnapshotIds().stream().map(SnapshotId::getUUID).collect(Collectors.toList());
        for (String prefix : new String[]{"snap-", "meta-"}) {
                final Collection<String> foundSnapshotUUIDs = repoRoot.listBlobs().keySet().stream().filter(p -> p.startsWith(prefix))
                    .map(p -> p.replace(prefix, "").replace(".dat", ""))
                    .collect(Collectors.toSet());
                assertThat(foundSnapshotUUIDs, containsInAnyOrder(expectedSnapshotUUIDs.toArray(Strings.EMPTY_ARRAY)));
        }
    }

    public long createDanglingIndex(String name, Set<String> files) throws InterruptedException,
            ExecutionException {
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        final AtomicLong totalSize = new AtomicLong();
        executor.execute(new ActionRunnable<>(future) {
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

    public void assertCorruptionVisible(Map<String, Set<String>> indexToFiles) throws Exception {
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        executor.execute(new ActionRunnable<>(future) {
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
                        if (blobStore.blobContainer(repository.basePath().add("indices").add(index))
                                .blobExists(file) == false) {
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

    public void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetaData> blobs) throws Exception {
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
