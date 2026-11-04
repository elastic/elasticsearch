/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.repositories.FinalizeSnapshotContext.UpdatedShardGenerations;
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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

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

    public void testXContent() throws IOException {
        RepositoryData repositoryData = generateRandomRepoData().withClusterUuid(UUIDs.randomBase64UUID(random()));
        XContentBuilder builder = JsonXContent.contentBuilder();
        repositoryData.snapshotsToXContent(builder, IndexVersion.current());
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
                randomFrom(IndexVersion.current(), IndexVersions.MINIMUM_COMPATIBLE),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomAlphaOfLength(10)
            ),
            new UpdatedShardGenerations(shardGenerations, ShardGenerations.EMPTY),
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

    /**
     * A name new to the repository whose index was snapshotted before under another name (a renamed index) resolves to that
     * earlier id, so the blobs stay under one path; a name whose index is unknown gets a fresh id; and the shard generations of
     * the earlier name are visible through the new one.
     */
    public void testResolveNewIndicesReusesIdOfRenamedIndex() {
        final IndexId oldName = new IndexId("old-name", UUIDs.randomBase64UUID(random()));
        final String indexUuid = UUIDs.randomBase64UUID(random());
        final ShardGeneration generation = ShardGeneration.newGeneration(random());
        final String identifier = indexUuid + "-" + UUIDs.randomBase64UUID(random()) + "-1-1-1";
        final SnapshotId snapshotId = new SnapshotId("snap", UUIDs.randomBase64UUID(random()));
        final RepositoryData repositoryData = RepositoryData.EMPTY.addSnapshot(
            snapshotId,
            new RepositoryData.SnapshotDetails(SnapshotState.SUCCESS, IndexVersion.current(), 1L, 2L, null),
            new UpdatedShardGenerations(ShardGenerations.builder().put(oldName, 0, generation).build(), ShardGenerations.EMPTY),
            Map.of(oldName, identifier),
            Map.of(identifier, UUIDs.randomBase64UUID(random()))
        );

        final Map<String, IndexId> resolved = repositoryData.resolveNewIndices(
            List.of("old-name", "new-name", "unrelated"),
            Map.of(),
            name -> switch (name) {
                case "new-name" -> indexUuid;
                case "unrelated" -> UUIDs.randomBase64UUID(random());
                default -> null;
            }
        );
        assertThat(resolved.get("old-name"), equalTo(oldName));
        assertThat(resolved.get("new-name").getId(), equalTo(oldName.getId()));
        assertThat(resolved.get("unrelated").getId(), not(equalTo(oldName.getId())));
        assertThat(repositoryData.indexIdForIndexUuid(indexUuid), equalTo(oldName));
        assertNull(repositoryData.indexIdForIndexUuid(UUIDs.randomBase64UUID(random())));
        assertTrue(repositoryData.hasIndexId(oldName.getId()));

        // the new name sees the old name's shard generation, and an update under either name is seen by both
        final IndexId newName = resolved.get("new-name");
        assertThat(repositoryData.shardGenerations().getShardGen(newName, 0), equalTo(generation));
        final ShardGeneration updated = ShardGeneration.newGeneration(random());
        final ShardGenerations shardGenerations = ShardGenerations.builder()
            .putAll(repositoryData.shardGenerations())
            .put(newName, 0, updated)
            .build();
        assertThat(shardGenerations.getShardGen(oldName, 0), equalTo(updated));
        assertThat(shardGenerations.getShardGen(newName, 0), equalTo(updated));
        assertThat(shardGenerations.indices(), containsInAnyOrder(oldName, newName));
    }

    /**
     * A repository of {@code indices} indices snapshotted twice, the second time with {@code renamed} of them under another name and
     * the same repository id. The index UUID of each id is recorded in {@code uuidById}, and two further indices have UUIDs of which
     * one is a prefix of the other, which the metadata identifiers cannot tell apart.
     */
    private RepositoryData repositoryDataWithRenamedIndices(int indices, int renamed, Map<String, String> uuidById) {
        final ShardGenerations.Builder firstGenerations = ShardGenerations.builder();
        final Map<IndexId, String> firstIdentifiers = new HashMap<>();
        final Map<String, String> newIdentifiers = new HashMap<>();
        final List<IndexId> original = new ArrayList<>();
        for (int i = 0; i < indices + 2; i++) {
            final IndexId indexId = new IndexId("index-" + i, UUIDs.randomBase64UUID(random()));
            // the last two UUIDs: "ab" and "ab-cd", whose identifiers both start with "ab-"
            final String uuid = i == indices ? "ab" : i == indices + 1 ? "ab-cd" : UUIDs.randomBase64UUID(random());
            final String identifier = uuid + "-" + UUIDs.randomBase64UUID(random()) + "-1-1-1";
            original.add(indexId);
            uuidById.put(indexId.getId(), uuid);
            firstIdentifiers.put(indexId, identifier);
            newIdentifiers.put(identifier, UUIDs.randomBase64UUID(random()));
            firstGenerations.put(indexId, 0, ShardGeneration.newGeneration(random()));
        }
        RepositoryData data = RepositoryData.EMPTY.addSnapshot(
            new SnapshotId("first", UUIDs.randomBase64UUID(random())),
            new RepositoryData.SnapshotDetails(SnapshotState.SUCCESS, IndexVersion.current(), 1L, 2L, null),
            new UpdatedShardGenerations(firstGenerations.build(), ShardGenerations.EMPTY),
            firstIdentifiers,
            newIdentifiers
        );
        final ShardGenerations.Builder secondGenerations = ShardGenerations.builder();
        final Map<IndexId, String> secondIdentifiers = new HashMap<>();
        for (int i = 0; i < original.size(); i++) {
            final IndexId before = original.get(i);
            final IndexId indexId = i < renamed ? new IndexId("renamed-" + i, before.getId()) : before;
            secondIdentifiers.put(indexId, firstIdentifiers.get(before));
            secondGenerations.put(indexId, 0, ShardGeneration.newGeneration(random()));
        }
        return data.addSnapshot(
            new SnapshotId("second", UUIDs.randomBase64UUID(random())),
            new RepositoryData.SnapshotDetails(SnapshotState.SUCCESS, IndexVersion.current(), 3L, 4L, null),
            new UpdatedShardGenerations(secondGenerations.build(), ShardGenerations.EMPTY),
            secondIdentifiers,
            Map.of()
        );
    }

    /**
     * {@code hasIndexId} and {@code indexIdsSharingId} are answered from the indices grouped by id once per instance. They must agree
     * with a scan of the indices, for ids with one name, ids that two names share and ids the repository does not have.
     */
    public void testIndexIdLookupsAgreeWithAScan() {
        final Map<String, String> uuidById = new HashMap<>();
        final int indices = randomIntBetween(2, 40);
        final RepositoryData data = repositoryDataWithRenamedIndices(indices, randomIntBetween(0, indices), uuidById);

        final List<String> ids = new ArrayList<>(uuidById.keySet());
        ids.add(UUIDs.randomBase64UUID(random()));
        for (String id : ids) {
            final List<IndexId> scanned = data.getIndices().values().stream().filter(indexId -> indexId.getId().equals(id)).toList();
            assertThat(data.hasIndexId(id), equalTo(scanned.isEmpty() == false));
            assertThat(data.indexIdsSharingId(id), containsInAnyOrder(scanned.toArray()));
        }
        // the names that share an id are listed together
        final long idsWithTwoNames = uuidById.keySet().stream().filter(id -> data.indexIdsSharingId(id).size() == 2).count();
        assertThat(idsWithTwoNames, equalTo(data.getIndices().size() - (long) uuidById.size()));
    }

    /**
     * {@code indexIdForIndexUuid} finds the index id through the sorted metadata identifiers. It must find what a scan of every
     * snapshot's identifiers finds, including for UUIDs that contain dashes and for a UUID that is a prefix of another one, and
     * nothing for a UUID the repository does not know.
     */
    public void testIndexIdForIndexUuidAgreesWithAScan() {
        final Map<String, String> uuidById = new HashMap<>();
        final int indices = randomIntBetween(2, 40);
        final RepositoryData data = repositoryDataWithRenamedIndices(indices, randomIntBetween(0, indices), uuidById);

        final List<String> uuids = new ArrayList<>(uuidById.values());
        uuids.add(UUIDs.randomBase64UUID(random()));
        uuids.add("a");
        for (String uuid : uuids) {
            final String prefix = uuid + "-";
            final Set<IndexId> matching = new HashSet<>();
            for (Map<IndexId, String> identifiersBySnapshot : data.indexMetaDataGenerations().lookup.values()) {
                for (Map.Entry<IndexId, String> entry : identifiersBySnapshot.entrySet()) {
                    if (entry.getValue().startsWith(prefix) && data.getIndices().containsKey(entry.getKey().getName())) {
                        matching.add(entry.getKey());
                    }
                }
            }
            final IndexId found = data.indexIdForIndexUuid(uuid);
            if (matching.isEmpty()) {
                assertNull("found [" + found + "] for [" + uuid + "]", found);
            } else {
                assertThat("nothing found for [" + uuid + "]", matching, hasItem(found));
            }
        }
    }

    /**
     * Parsing a repository in which two names share an id picks, for each snapshot, the name that snapshot references, and does not
     * change what is read for ids that have a single name.
     */
    public void testXContentWithNamesSharingAnId() throws IOException {
        final Map<String, String> uuidById = new HashMap<>();
        final int indices = randomIntBetween(2, 20);
        final RepositoryData data = repositoryDataWithRenamedIndices(indices, randomIntBetween(1, indices), uuidById).withClusterUuid(
            UUIDs.randomBase64UUID(random())
        );
        final XContentBuilder builder = JsonXContent.contentBuilder();
        data.snapshotsToXContent(builder, IndexVersion.current());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final RepositoryData parsed = RepositoryData.snapshotsFromXContent(parser, data.getGenId(), randomBoolean());
            assertEquals(data, parsed);
            assertEquals(data.indexMetaDataGenerations(), parsed.indexMetaDataGenerations());
            for (SnapshotId snapshotId : data.getSnapshotIds()) {
                for (IndexId indexId : data.getIndices().values()) {
                    // every index id is read back with the metadata blob it had
                    assertThat(
                        parsed.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId),
                        equalTo(data.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId))
                    );
                }
            }
        }
    }

    /**
     * Every index is renamed in the middle of a run of snapshots, so the earlier snapshots reference the old names and the later ones
     * the new names of the same ids. Parsing must give each snapshot the name it references, however many snapshots there are.
     */
    public void testXContentWithRenameInTheMiddleOfManySnapshots() throws IOException {
        final int indices = randomIntBetween(2, 12);
        final int snapshots = randomIntBetween(4, 40);
        final int renameAt = randomIntBetween(1, snapshots - 1);
        final List<String> uuids = new ArrayList<>();
        final List<String> ids = new ArrayList<>();
        final Map<String, String> newIdentifiers = new HashMap<>();
        final Map<Integer, String> identifierOfIndex = new HashMap<>();
        for (int i = 0; i < indices; i++) {
            uuids.add(UUIDs.randomBase64UUID(random()));
            ids.add(UUIDs.randomBase64UUID(random()));
            final String identifier = uuids.get(i) + "-" + UUIDs.randomBase64UUID(random()) + "-1-1-1";
            identifierOfIndex.put(i, identifier);
            newIdentifiers.put(identifier, UUIDs.randomBase64UUID(random()));
        }
        RepositoryData data = RepositoryData.EMPTY;
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        final List<Map<IndexId, String>> expectedIdentifiers = new ArrayList<>();
        for (int s = 0; s < snapshots; s++) {
            final ShardGenerations.Builder generations = ShardGenerations.builder();
            final Map<IndexId, String> identifiers = new HashMap<>();
            for (int i = 0; i < indices; i++) {
                final IndexId indexId = new IndexId((s < renameAt ? "old-" : "new-") + i, ids.get(i));
                identifiers.put(indexId, identifierOfIndex.get(i));
                generations.put(indexId, 0, ShardGeneration.newGeneration(random()));
            }
            final SnapshotId snapshotId = new SnapshotId("snapshot-" + s, UUIDs.randomBase64UUID(random()));
            snapshotIds.add(snapshotId);
            expectedIdentifiers.add(identifiers);
            data = data.addSnapshot(
                snapshotId,
                new RepositoryData.SnapshotDetails(SnapshotState.SUCCESS, IndexVersion.current(), s, s + 1L, null),
                new UpdatedShardGenerations(generations.build(), ShardGenerations.EMPTY),
                identifiers,
                s == 0 ? newIdentifiers : Map.of()
            );
        }
        data = data.withClusterUuid(UUIDs.randomBase64UUID(random()));
        final XContentBuilder builder = JsonXContent.contentBuilder();
        data.snapshotsToXContent(builder, IndexVersion.current());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final RepositoryData parsed = RepositoryData.snapshotsFromXContent(parser, data.getGenId(), randomBoolean());
            assertEquals(data, parsed);
            for (int s = 0; s < snapshots; s++) {
                for (Map.Entry<IndexId, String> expected : expectedIdentifiers.get(s).entrySet()) {
                    // the snapshot references this name of the index, and the metadata it recorded for it is the one read back
                    assertThat(parsed.getSnapshots(expected.getKey()), hasItem(snapshotIds.get(s)));
                    assertThat(
                        parsed.indexMetaDataGenerations().indexMetaBlobId(snapshotIds.get(s), expected.getKey()),
                        equalTo(newIdentifiers.get(expected.getValue()))
                    );
                }
            }
        }
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
                    randomFrom(IndexVersion.current(), IndexVersions.MINIMUM_COMPATIBLE),
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
                randomFrom(IndexVersion.current(), IndexVersions.MINIMUM_COMPATIBLE),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomAlphaOfLength(10)
            ),
            UpdatedShardGenerations.EMPTY,
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
        repositoryData.snapshotsToXContent(builder, IndexVersion.current());
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
        corruptedRepositoryData.snapshotsToXContent(corruptedBuilder, IndexVersion.current());

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

    public void testFailsIfMinVersionNotSatisfied() throws IOException {
        final String futureVersion = Version.fromId(IndexVersion.current().id() + 1_000_000).toString();

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

    public void testToString() {
        final var repositoryData = generateRandomRepoData();
        assertThat(
            repositoryData.toString(),
            allOf(
                containsString("RepositoryData"),
                containsString(repositoryData.getUuid()),
                containsString(Long.toString(repositoryData.getGenId())),
                not(containsString("@")) // not the default Object#toString which does a very expensive hashcode computation
            )
        );
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
                    randomFrom(IndexVersion.current(), IndexVersions.MINIMUM_COMPATIBLE),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomAlphaOfLength(10)
                ),
                new UpdatedShardGenerations(builder.build(), ShardGenerations.EMPTY),
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
