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

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.blobId;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.parseNameUUIDFromBlobName;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link BlobStoreRepository} and its subclasses.
 */
public class BlobStoreRepositoryTests extends ESSingleNodeTestCase {

    public void testRetrieveSnapshots() throws Exception {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse =
            client.admin().cluster().preparePutRepository(repositoryName)
                                    .setType("fs")
                                    .setSettings(Settings.builder().put(node().settings()).put("location", location))
                                    .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client().prepareIndex(indexName, "type1", id).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush(indexName).setWaitIfOngoing(true).get();

        logger.info("--> create first snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
                                                              .cluster()
                                                              .prepareCreateSnapshot(repositoryName, "test-snap-1")
                                                              .setWaitForCompletion(true)
                                                              .setIndices(indexName)
                                                              .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> create second snapshot");
        createSnapshotResponse = client.admin()
                                       .cluster()
                                       .prepareCreateSnapshot(repositoryName, "test-snap-2")
                                       .setWaitForCompletion(true)
                                       .setIndices(indexName)
                                       .get();
        final SnapshotId snapshotId2 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        @SuppressWarnings("unchecked") final BlobStoreRepository repository =
            (BlobStoreRepository) repositoriesService.repository(repositoryName);
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2);

        List<SnapshotId> snapshotIds = repository.snapshots().stream()
                                                             .sorted((s1, s2) -> s1.getName().compareTo(s2.getName()))
                                                             .collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    public void testSnapshotIndexFile() throws Exception {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        PutRepositoryResponse putRepositoryResponse =
            client.admin().cluster().preparePutRepository(repositoryName)
                                    .setType("fs")
                                    .setSettings(Settings.builder().put(node().settings()).put("location", location))
                                    .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        @SuppressWarnings("unchecked") final BlobStoreRepository repository =
            (BlobStoreRepository) repositoriesService.repository(repositoryName);

        // write to and read from a snapshot file with no entries
        repository.writeSnapshotList(Collections.emptyList());
        List<SnapshotId> readSnapshotIds = repository.readSnapshotList();
        assertThat(readSnapshotIds.size(), equalTo(0));

        // write to and read from a snapshot file with a random number of entries
        final int numSnapshots = randomIntBetween(1, 1000);
        final List<SnapshotId> snapshotIds = new ArrayList<>(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            snapshotIds.add(new SnapshotId(randomAsciiOfLength(8), UUIDs.randomBase64UUID()));
        }
        repository.writeSnapshotList(snapshotIds);
        readSnapshotIds = repository.readSnapshotList();
        assertThat(readSnapshotIds, equalTo(snapshotIds));
    }

    public void testOldIndexFileFormat() throws Exception {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        PutRepositoryResponse putRepositoryResponse =
            client.admin().cluster().preparePutRepository(repositoryName)
                .setType("fs")
                .setSettings(Settings.builder().put(node().settings()).put("location", location))
                .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        @SuppressWarnings("unchecked") final BlobStoreRepository repository =
            (BlobStoreRepository) repositoriesService.repository(repositoryName);

        // write old index file format
        final int numOldSnapshots = randomIntBetween(1, 50);
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        for (int i = 0; i < numOldSnapshots; i++) {
            snapshotIds.add(new SnapshotId(randomAsciiOfLength(8), SnapshotId.UNASSIGNED_UUID));
        }
        writeOldFormat(repository, snapshotIds.stream().map(SnapshotId::getName).collect(Collectors.toList()));
        List<SnapshotId> readSnapshotIds = repository.readSnapshotList();
        assertThat(Sets.newHashSet(readSnapshotIds), equalTo(Sets.newHashSet(snapshotIds)));

        // write to and read from a snapshot file with a random number of new entries added
        final int numSnapshots = randomIntBetween(1, 1000);
        for (int i = 0; i < numSnapshots; i++) {
            snapshotIds.add(new SnapshotId(randomAsciiOfLength(8), UUIDs.randomBase64UUID()));
        }
        repository.writeSnapshotList(snapshotIds);
        readSnapshotIds = repository.readSnapshotList();
        assertThat(Sets.newHashSet(readSnapshotIds), equalTo(Sets.newHashSet(snapshotIds)));
    }

    public void testParseUUIDFromBlobName() {
        String blobStr = "abc123";
        Tuple<String, String> pair = parseNameUUIDFromBlobName(blobStr);
        assertThat(pair.v1(), equalTo(blobStr)); // snapshot name
        assertThat(pair.v2(), equalTo(SnapshotId.UNASSIGNED_UUID)); // snapshot uuid
        blobStr = "abcefghijklmnopqrstuvwxyz";
        pair = parseNameUUIDFromBlobName(blobStr);
        assertThat(pair.v1(), equalTo(blobStr));
        assertThat(pair.v2(), equalTo(SnapshotId.UNASSIGNED_UUID));
        blobStr = "abc123-xyz"; // not enough characters after '-' to have a uuid
        pair = parseNameUUIDFromBlobName(blobStr);
        assertThat(pair.v1(), equalTo(blobStr));
        assertThat(pair.v2(), equalTo(SnapshotId.UNASSIGNED_UUID));
        blobStr = "abc123-a1b2c3d4e5f6g7h8i9j0k1";
        pair = parseNameUUIDFromBlobName(blobStr);
        assertThat(pair.v1(), equalTo("abc123"));
        assertThat(pair.v2(), equalTo("a1b2c3d4e5f6g7h8i9j0k1"));
    }

    public void testBlobId() {
        SnapshotId snapshotId = new SnapshotId("abc123", SnapshotId.UNASSIGNED_UUID);
        assertThat(blobId(snapshotId), equalTo("abc123")); // just the snapshot name
        snapshotId = new SnapshotId("abc-123", SnapshotId.UNASSIGNED_UUID);
        assertThat(blobId(snapshotId), equalTo("abc-123")); // just the snapshot name
        String uuid = UUIDs.randomBase64UUID();
        snapshotId = new SnapshotId("abc123", uuid);
        assertThat(blobId(snapshotId), equalTo("abc123-" + uuid)); // snapshot name + '-' + uuid
        uuid = UUIDs.randomBase64UUID();
        snapshotId = new SnapshotId("abc-123", uuid);
        assertThat(blobId(snapshotId), equalTo("abc-123-" + uuid)); // snapshot name + '-' + uuid
    }

    private void writeOldFormat(final BlobStoreRepository repository, final List<String> snapshotNames) throws Exception {
        final BytesReference bRef;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            try (StreamOutput stream = new OutputStreamStreamOutput(bStream)) {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
                builder.startObject();
                builder.startArray("snapshots");
                for (final String snapshotName : snapshotNames) {
                    builder.value(snapshotName);
                }
                builder.endArray();
                builder.endObject();
                builder.close();
            }
            bRef = bStream.bytes();
        }
        repository.blobContainer().writeBlob(BlobStoreRepository.SNAPSHOTS_FILE, bRef); // write to index file
    }

}
