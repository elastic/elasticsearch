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

package org.elasticsearch.repositories;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for the {@link RepositoryData} class.
 */
public class RepositoryDataTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        RepositoryData repositoryData1 = generateRandomRepoData();
        RepositoryData repositoryData2 = repositoryData1.copy();
        assertEquals(repositoryData1, repositoryData2);
        assertEquals(repositoryData1.hashCode(), repositoryData2.hashCode());
    }

    public void testIndicesToUpdateAfterRemovingSnapshot() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final List<IndexId> indicesBefore = List.copyOf(repositoryData.getIndices().values());
        final SnapshotId randomSnapshot = randomFrom(repositoryData.getSnapshotIds());
        final IndexId[] indicesToUpdate = indicesBefore.stream().filter(index -> {
            final List<SnapshotId> snapshotIds = repositoryData.getSnapshots(index);
            return snapshotIds.contains(randomSnapshot) && snapshotIds.size() > 1;
        }).toArray(IndexId[]::new);
        assertThat(repositoryData.indicesToUpdateAfterRemovingSnapshot(
            Collections.singleton(randomSnapshot)), containsInAnyOrder(indicesToUpdate));
    }

    public void testXContent() throws IOException {
        RepositoryData repositoryData = generateRandomRepoData();
        XContentBuilder builder = JsonXContent.contentBuilder();
        repositoryData.snapshotsToXContent(builder, true);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            long gen = (long) randomIntBetween(0, 500);
            RepositoryData fromXContent = RepositoryData.snapshotsFromXContent(parser, gen);
            assertEquals(repositoryData, fromXContent);
            assertEquals(gen, fromXContent.getGenId());
        }
    }

    public void testAddSnapshots() {
        RepositoryData repositoryData = generateRandomRepoData();
        // test that adding the same snapshot id to the repository data throws an exception
        Map<String, IndexId> indexIdMap = repositoryData.getIndices();
        // test that adding a snapshot and its indices works
        SnapshotId newSnapshot = new SnapshotId(randomAlphaOfLength(7), UUIDs.randomBase64UUID());
        List<IndexId> indices = new ArrayList<>();
        Set<IndexId> newIndices = new HashSet<>();
        int numNew = randomIntBetween(1, 10);
        final ShardGenerations.Builder builder = ShardGenerations.builder();
        for (int i = 0; i < numNew; i++) {
            IndexId indexId = new IndexId(randomAlphaOfLength(7), UUIDs.randomBase64UUID());
            newIndices.add(indexId);
            indices.add(indexId);
            builder.put(indexId, 0, "1");
        }
        int numOld = randomIntBetween(1, indexIdMap.size());
        List<String> indexNames = new ArrayList<>(indexIdMap.keySet());
        for (int i = 0; i < numOld; i++) {
            final IndexId indexId = indexIdMap.get(indexNames.get(i));
            indices.add(indexId);
            builder.put(indexId, 0, "2");
        }
        RepositoryData newRepoData = repositoryData.addSnapshot(newSnapshot,
            randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED),
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()), builder.build());
        // verify that the new repository data has the new snapshot and its indices
        assertTrue(newRepoData.getSnapshotIds().contains(newSnapshot));
        for (IndexId indexId : indices) {
            List<SnapshotId> snapshotIds = newRepoData.getSnapshots(indexId);
            assertTrue(snapshotIds.contains(newSnapshot));
            if (newIndices.contains(indexId)) {
                assertEquals(snapshotIds.size(), 1); // if it was a new index, only the new snapshot should be in its set
            }
        }
        assertEquals(repositoryData.getGenId(), newRepoData.getGenId());
    }

    public void testInitIndices() {
        final int numSnapshots = randomIntBetween(1, 30);
        final Map<String, SnapshotId> snapshotIds = new HashMap<>(numSnapshots);
        final Map<String, SnapshotState> snapshotStates = new HashMap<>(numSnapshots);
        final Map<String, Version> snapshotVersions = new HashMap<>(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            snapshotIds.put(snapshotId.getUUID(), snapshotId);
            snapshotStates.put(snapshotId.getUUID(), randomFrom(SnapshotState.values()));
            snapshotVersions.put(snapshotId.getUUID(), randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()));
        }
        RepositoryData repositoryData = new RepositoryData(EMPTY_REPO_GEN, snapshotIds,
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), ShardGenerations.EMPTY);
        // test that initializing indices works
        Map<IndexId, List<SnapshotId>> indices = randomIndices(snapshotIds);
        RepositoryData newRepoData =
            new RepositoryData(repositoryData.getGenId(), snapshotIds, snapshotStates, snapshotVersions, indices, ShardGenerations.EMPTY);
        List<SnapshotId> expected = new ArrayList<>(repositoryData.getSnapshotIds());
        Collections.sort(expected);
        List<SnapshotId> actual = new ArrayList<>(newRepoData.getSnapshotIds());
        Collections.sort(actual);
        assertEquals(expected, actual);
        for (IndexId indexId : indices.keySet()) {
            assertEquals(indices.get(indexId), newRepoData.getSnapshots(indexId));
        }
    }

    public void testRemoveSnapshot() {
        RepositoryData repositoryData = generateRandomRepoData();
        List<SnapshotId> snapshotIds = new ArrayList<>(repositoryData.getSnapshotIds());
        assertThat(snapshotIds.size(), greaterThan(0));
        SnapshotId removedSnapshotId = snapshotIds.remove(randomIntBetween(0, snapshotIds.size() - 1));
        RepositoryData newRepositoryData = repositoryData.removeSnapshots(Collections.singleton(removedSnapshotId), ShardGenerations.EMPTY);
        // make sure the repository data's indices no longer contain the removed snapshot
        for (final IndexId indexId : newRepositoryData.getIndices().values()) {
            assertFalse(newRepositoryData.getSnapshots(indexId).contains(removedSnapshotId));
        }
    }

    public void testResolveIndexId() {
        RepositoryData repositoryData = generateRandomRepoData();
        Map<String, IndexId> indices = repositoryData.getIndices();
        Set<String> indexNames = indices.keySet();
        assertThat(indexNames.size(), greaterThan(0));
        String indexName = indexNames.iterator().next();
        IndexId indexId = indices.get(indexName);
        assertEquals(indexId, repositoryData.resolveIndexId(indexName));
    }

    public void testGetSnapshotState() {
        final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
        final SnapshotState state = randomFrom(SnapshotState.values());
        final RepositoryData repositoryData = RepositoryData.EMPTY.addSnapshot(snapshotId, state,
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()), ShardGenerations.EMPTY);
        assertEquals(state, repositoryData.getSnapshotState(snapshotId));
        assertNull(repositoryData.getSnapshotState(new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID())));
    }

    public void testIndexThatReferencesAnUnknownSnapshot() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        final RepositoryData repositoryData = generateRandomRepoData();

        XContentBuilder builder = XContentBuilder.builder(xContent);
        repositoryData.snapshotsToXContent(builder, true);
        RepositoryData parsedRepositoryData;
        try (XContentParser xParser = createParser(builder)) {
            parsedRepositoryData = RepositoryData.snapshotsFromXContent(xParser, repositoryData.getGenId());
        }
        assertEquals(repositoryData, parsedRepositoryData);

        Map<String, SnapshotId> snapshotIds = new HashMap<>();
        Map<String, SnapshotState> snapshotStates = new HashMap<>();
        Map<String, Version> snapshotVersions = new HashMap<>();
        for (SnapshotId snapshotId : parsedRepositoryData.getSnapshotIds()) {
            snapshotIds.put(snapshotId.getUUID(), snapshotId);
            snapshotStates.put(snapshotId.getUUID(), parsedRepositoryData.getSnapshotState(snapshotId));
            snapshotVersions.put(snapshotId.getUUID(), parsedRepositoryData.getVersion(snapshotId));
        }

        final IndexId corruptedIndexId = randomFrom(parsedRepositoryData.getIndices().values());

        Map<IndexId, List<SnapshotId>> indexSnapshots = new HashMap<>();
        final ShardGenerations.Builder shardGenBuilder = ShardGenerations.builder();
        for (Map.Entry<String, IndexId> snapshottedIndex : parsedRepositoryData.getIndices().entrySet()) {
            IndexId indexId = snapshottedIndex.getValue();
            List<SnapshotId> snapshotsIds = new ArrayList<>(parsedRepositoryData.getSnapshots(indexId));
            if (corruptedIndexId.equals(indexId)) {
                snapshotsIds.add(new SnapshotId("_uuid", "_does_not_exist"));
            }
            indexSnapshots.put(indexId, snapshotsIds);
            final int shardCount = randomIntBetween(1, 10);
            for (int i = 0; i < shardCount; ++i) {
                shardGenBuilder.put(indexId, i, UUIDs.randomBase64UUID(random()));
            }
        }
        assertNotNull(corruptedIndexId);

        RepositoryData corruptedRepositoryData = new RepositoryData(parsedRepositoryData.getGenId(), snapshotIds, snapshotStates,
            snapshotVersions, indexSnapshots, shardGenBuilder.build());

        final XContentBuilder corruptedBuilder = XContentBuilder.builder(xContent);
        corruptedRepositoryData.snapshotsToXContent(corruptedBuilder, true);

        try (XContentParser xParser = createParser(corruptedBuilder)) {
            ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () ->
                RepositoryData.snapshotsFromXContent(xParser, corruptedRepositoryData.getGenId()));
            assertThat(e.getMessage(), equalTo("Detected a corrupted repository, index " + corruptedIndexId + " references an unknown " +
                "snapshot uuid [_does_not_exist]"));
        }
    }

    public void testIndexThatReferenceANullSnapshot() throws IOException {
        final XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.JSON).xContent());
        builder.startObject();
        {
            builder.startArray("snapshots");
            builder.value(new SnapshotId("_name", "_uuid"));
            builder.endArray();

            builder.startObject("indices");
            {
                builder.startObject("docs");
                {
                    builder.field("id", "_id");
                    builder.startArray("snapshots");
                    {
                        builder.startObject();
                        if (randomBoolean()) {
                            builder.field("name", "_name");
                        }
                        builder.endObject();
                    }
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        try (XContentParser xParser = createParser(builder)) {
            ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () ->
                RepositoryData.snapshotsFromXContent(xParser, randomNonNegativeLong()));
            assertThat(e.getMessage(), equalTo("Detected a corrupted repository, " +
                "index [docs/_id] references an unknown snapshot uuid [null]"));
        }
    }

    public static RepositoryData generateRandomRepoData() {
        final int numIndices = randomIntBetween(1, 30);
        final List<IndexId> indices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()));
        }
        final int numSnapshots = randomIntBetween(1, 30);
        RepositoryData repositoryData = RepositoryData.EMPTY;
        for (int i = 0; i < numSnapshots; i++) {
            final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            final List<IndexId> someIndices = indices.subList(0, randomIntBetween(1, numIndices));
            final ShardGenerations.Builder builder = ShardGenerations.builder();
            for (IndexId someIndex : someIndices) {
                final int shardCount = randomIntBetween(1, 10);
                for (int j = 0; j < shardCount; ++j) {
                    final String uuid = randomBoolean() ? null : UUIDs.randomBase64UUID(random());
                    builder.put(someIndex, j, uuid);
                }
            }
            repositoryData = repositoryData.addSnapshot(snapshotId, randomFrom(SnapshotState.values()),
                randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()), builder.build());
        }
        return repositoryData;
    }

    private static Map<IndexId, List<SnapshotId>> randomIndices(final Map<String, SnapshotId> snapshotIdsMap) {
        final List<SnapshotId> snapshotIds = new ArrayList<>(snapshotIdsMap.values());
        final int totalSnapshots = snapshotIds.size();
        final int numIndices = randomIntBetween(1, 30);
        final Map<IndexId, List<SnapshotId>> indices = new HashMap<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            final Set<SnapshotId> indexSnapshots = new LinkedHashSet<>();
            final int numIndicesForSnapshot = randomIntBetween(1, numIndices);
            for (int j = 0; j < numIndicesForSnapshot; j++) {
                indexSnapshots.add(snapshotIds.get(randomIntBetween(0, totalSnapshots - 1)));
            }
            indices.put(indexId, List.copyOf(indexSnapshots));
        }
        return indices;
    }
}
