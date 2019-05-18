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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public final class BlobStoreTestUtil {

    /**
     * Assert that there are no unreferenced indices or unreferenced root-level metadata blobs in any repository.
     * TODO: Expand the logic here to also check for unreferenced segment blobs and shard level metadata
     */
    public static void assertConsistency(BlobStoreRepository repository, Executor executor) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        executor.execute(new AbstractRunnable() {
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
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onAfter() {
                latch.countDown();
            }
        });
        assertTrue(latch.await(1L, TimeUnit.MINUTES));
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
}
