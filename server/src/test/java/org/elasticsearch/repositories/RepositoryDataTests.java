/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.elasticsearch.repositories.RepositoryData.MISSING_UUID;
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
        assertThat(
            repositoryData.indicesToUpdateAfterRemovingSnapshot(Collections.singleton(randomSnapshot)),
            containsInAnyOrder(indicesToUpdate)
        );
    }

    public void testXContent() throws IOException {
        RepositoryData repositoryData = generateRandomRepoData().withClusterUuid(UUIDs.randomBase64UUID(random()));
        XContentBuilder builder = JsonXContent.contentBuilder();
        repositoryData.snapshotsToXContent(builder, Version.CURRENT);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            long gen = (long) randomIntBetween(0, 500);
            RepositoryData fromXContent = RepositoryData.snapshotsFromXContent(parser, gen, randomBoolean());
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
            builder.put(indexId, 0, ShardGeneration.newGeneration(random()));
        }
        int numOld = randomIntBetween(1, indexIdMap.size());
        List<String> indexNames = new ArrayList<>(indexIdMap.keySet());
        for (int i = 0; i < numOld; i++) {
            final IndexId indexId = indexIdMap.get(indexNames.get(i));
            indices.add(indexId);
            builder.put(indexId, 0, ShardGeneration.newGeneration(random()));
        }
        final ShardGenerations shardGenerations = builder.build();
        final Map<IndexId, String> indexLookup = shardGenerations.indices()
            .stream()
            .collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
        RepositoryData newRepoData = repositoryData.addSnapshot(
            newSnapshot,
            new RepositoryData.SnapshotDetails(
                randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED),
                randomFrom(IndexVersion.current(), Version.CURRENT.minimumCompatibilityVersion().indexVersion),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomAlphaOfLength(10)
            ),
            shardGenerations,
            indexLookup,
            indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random())))
        );
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
        final Map<String, SnapshotId> snapshotIds = Maps.newMapWithExpectedSize(numSnapshots);
        final Map<String, RepositoryData.SnapshotDetails> snapshotsDetails = Maps.newMapWithExpectedSize(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            snapshotIds.put(snapshotId.getUUID(), snapshotId);
            snapshotsDetails.put(
                snapshotId.getUUID(),
                new RepositoryData.SnapshotDetails(
                    randomFrom(SnapshotState.values()),
                    randomFrom(IndexVersion.current(), Version.CURRENT.minimumCompatibilityVersion().indexVersion),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomAlphaOfLength(10)
                )
            );
        }
        RepositoryData repositoryData = new RepositoryData(
            MISSING_UUID,
            EMPTY_REPO_GEN,
            snapshotIds,
            Collections.emptyMap(),
            Collections.emptyMap(),
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY,
            MISSING_UUID
        );
        // test that initializing indices works
        Map<IndexId, List<SnapshotId>> indices = randomIndices(snapshotIds);
        RepositoryData newRepoData = new RepositoryData(
            UUIDs.randomBase64UUID(random()),
            repositoryData.getGenId(),
            snapshotIds,
            snapshotsDetails,
            indices,
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY,
            UUIDs.randomBase64UUID(random())
        );
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
        final RepositoryData repositoryData = RepositoryData.EMPTY.addSnapshot(
            snapshotId,
            new RepositoryData.SnapshotDetails(
                state,
                randomFrom(IndexVersion.current(), Version.CURRENT.minimumCompatibilityVersion().indexVersion),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomAlphaOfLength(10)
            ),
            ShardGenerations.EMPTY,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertEquals(state, repositoryData.getSnapshotState(snapshotId));
        assertNull(repositoryData.getSnapshotState(new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID())));
    }

    public void testIndexThatReferencesAnUnknownSnapshot() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        final RepositoryData repositoryData = generateRandomRepoData().withClusterUuid(UUIDs.randomBase64UUID(random()));

        XContentBuilder builder = XContentBuilder.builder(xContent);
        repositoryData.snapshotsToXContent(builder, Version.CURRENT);
        RepositoryData parsedRepositoryData;
        try (XContentParser xParser = createParser(builder)) {
            parsedRepositoryData = RepositoryData.snapshotsFromXContent(xParser, repositoryData.getGenId(), randomBoolean());
        }
        assertEquals(repositoryData, parsedRepositoryData);

        Map<String, SnapshotId> snapshotIds = new HashMap<>();
        Map<String, RepositoryData.SnapshotDetails> snapshotsDetails = new HashMap<>();
        for (SnapshotId snapshotId : parsedRepositoryData.getSnapshotIds()) {
            snapshotIds.put(snapshotId.getUUID(), snapshotId);
            snapshotsDetails.put(
                snapshotId.getUUID(),
                new RepositoryData.SnapshotDetails(
                    parsedRepositoryData.getSnapshotState(snapshotId),
                    parsedRepositoryData.getVersion(snapshotId),
                    parsedRepositoryData.getSnapshotDetails(snapshotId).getStartTimeMillis(),
                    parsedRepositoryData.getSnapshotDetails(snapshotId).getEndTimeMillis(),
                    randomAlphaOfLength(10)
                )
            );
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
                shardGenBuilder.put(indexId, i, ShardGeneration.newGeneration(random()));
            }
        }
        assertNotNull(corruptedIndexId);

        RepositoryData corruptedRepositoryData = new RepositoryData(
            parsedRepositoryData.getUuid(),
            parsedRepositoryData.getGenId(),
            snapshotIds,
            snapshotsDetails,
            indexSnapshots,
            shardGenBuilder.build(),
            IndexMetaDataGenerations.EMPTY,
            UUIDs.randomBase64UUID(random())
        );

        final XContentBuilder corruptedBuilder = XContentBuilder.builder(xContent);
        corruptedRepositoryData.snapshotsToXContent(corruptedBuilder, Version.CURRENT);

        try (XContentParser xParser = createParser(corruptedBuilder)) {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> RepositoryData.snapshotsFromXContent(xParser, corruptedRepositoryData.getGenId(), randomBoolean())
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "Detected a corrupted repository, index "
                        + corruptedIndexId
                        + " references an unknown "
                        + "snapshot uuid [_does_not_exist]"
                )
            );
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
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> RepositoryData.snapshotsFromXContent(xParser, randomNonNegativeLong(), randomBoolean())
            );
            assertThat(
                e.getMessage(),
                equalTo("Detected a corrupted repository, " + "index [docs/_id] references an unknown snapshot uuid [null]")
            );
        }
    }

    // Test removing snapshot from random data where no two snapshots share any index metadata blobs
    public void testIndexMetaDataToRemoveAfterRemovingSnapshotNoSharing() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final SnapshotId snapshotId = randomFrom(repositoryData.getSnapshotIds());
        final IndexMetaDataGenerations indexMetaDataGenerations = repositoryData.indexMetaDataGenerations();
        final Collection<IndexId> indicesToUpdate = repositoryData.indicesToUpdateAfterRemovingSnapshot(Collections.singleton(snapshotId));
        final Map<IndexId, Collection<String>> identifiersToRemove = indexMetaDataGenerations.lookup.get(snapshotId)
            .entrySet()
            .stream()
            .filter(e -> indicesToUpdate.contains(e.getKey()))
            .collect(
                Collectors.toMap(Map.Entry::getKey, e -> Collections.singleton(indexMetaDataGenerations.getIndexMetaBlobId(e.getValue())))
            );
        assertEquals(repositoryData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(snapshotId)), identifiersToRemove);
    }

    // Test removing snapshot from random data that has some or all index metadata shared
    public void testIndexMetaDataToRemoveAfterRemovingSnapshotWithSharing() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final ShardGenerations.Builder builder = ShardGenerations.builder();
        final SnapshotId otherSnapshotId = randomFrom(repositoryData.getSnapshotIds());
        final Collection<IndexId> indicesInOther = repositoryData.getIndices()
            .values()
            .stream()
            .filter(index -> repositoryData.getSnapshots(index).contains(otherSnapshotId))
            .collect(Collectors.toSet());
        for (IndexId indexId : indicesInOther) {
            builder.put(indexId, 0, ShardGeneration.newGeneration(random()));
        }
        final Map<IndexId, String> newIndices = new HashMap<>();
        final Map<String, String> newIdentifiers = new HashMap<>();
        final Map<IndexId, Collection<String>> removeFromOther = new HashMap<>();
        for (IndexId indexId : randomSubsetOf(repositoryData.getIndices().values())) {
            if (indicesInOther.contains(indexId)) {
                removeFromOther.put(
                    indexId,
                    Collections.singleton(repositoryData.indexMetaDataGenerations().indexMetaBlobId(otherSnapshotId, indexId))
                );
            }
            final String identifier = randomAlphaOfLength(20);
            newIndices.put(indexId, identifier);
            newIdentifiers.put(identifier, UUIDs.randomBase64UUID(random()));
            builder.put(indexId, 0, ShardGeneration.newGeneration(random()));
        }
        final ShardGenerations shardGenerations = builder.build();
        final Map<IndexId, String> indexLookup = new HashMap<>(repositoryData.indexMetaDataGenerations().lookup.get(otherSnapshotId));
        indexLookup.putAll(newIndices);
        final SnapshotId newSnapshot = new SnapshotId(randomAlphaOfLength(7), UUIDs.randomBase64UUID(random()));

        final RepositoryData.SnapshotDetails details = new RepositoryData.SnapshotDetails(
            SnapshotState.SUCCESS,
            IndexVersion.current(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomAlphaOfLength(10)
        );
        final RepositoryData newRepoData = repositoryData.addSnapshot(newSnapshot, details, shardGenerations, indexLookup, newIdentifiers);
        assertEquals(
            newRepoData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(newSnapshot)),
            newIndices.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.singleton(newIdentifiers.get(e.getValue()))))
        );
        assertEquals(newRepoData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(otherSnapshotId)), removeFromOther);
    }

    public void testFailsIfMinVersionNotSatisfied() throws IOException {
        final Version futureVersion = Version.fromString((Version.CURRENT.major + 1) + ".0.0");

        final XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.JSON).xContent());
        builder.startObject();
        {
            builder.field("min_version", futureVersion);
            builder.field("junk", "should not get this far");
        }
        builder.endObject();

        try (XContentParser xParser = createParser(builder)) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> RepositoryData.snapshotsFromXContent(xParser, randomNonNegativeLong(), randomBoolean())
            );
            assertThat(
                e.getMessage(),
                equalTo("this snapshot repository format requires Elasticsearch version [" + futureVersion + "] or later")
            );
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
                    final ShardGeneration shardGeneration = randomBoolean() ? null : ShardGeneration.newGeneration(random());
                    builder.put(someIndex, j, shardGeneration);
                }
            }
            final Map<IndexId, String> indexLookup = someIndices.stream()
                .collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
            repositoryData = repositoryData.addSnapshot(
                snapshotId,
                new RepositoryData.SnapshotDetails(
                    randomFrom(SnapshotState.values()),
                    randomFrom(IndexVersion.current(), Version.CURRENT.minimumCompatibilityVersion().indexVersion),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomAlphaOfLength(10)
                ),
                builder.build(),
                indexLookup,
                indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random())))
            );
        }
        return repositoryData;
    }

    private static Map<IndexId, List<SnapshotId>> randomIndices(final Map<String, SnapshotId> snapshotIdsMap) {
        final List<SnapshotId> snapshotIds = new ArrayList<>(snapshotIdsMap.values());
        final int totalSnapshots = snapshotIds.size();
        final int numIndices = randomIntBetween(1, 30);
        final Map<IndexId, List<SnapshotId>> indices = Maps.newMapWithExpectedSize(numIndices);
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
