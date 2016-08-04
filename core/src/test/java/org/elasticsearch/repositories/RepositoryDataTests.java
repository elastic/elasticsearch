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

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.snapshots.SnapshotId;
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

    public void testXContent() throws IOException {
        RepositoryData repositoryData = generateRandomRepoData();
        XContentBuilder builder = JsonXContent.contentBuilder();
        repositoryData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent().createParser(builder.bytes());
        assertEquals(repositoryData, RepositoryData.fromXContent(parser));
    }

    public void testAddSnapshots() {
        RepositoryData repositoryData = generateRandomRepoData();
        // test that adding the same snapshot id to the repository data throws an exception
        final SnapshotId snapshotId = repositoryData.getSnapshotIds().get(0);
        Map<String, IndexId> indexIdMap = repositoryData.getIndices();
        expectThrows(IllegalArgumentException.class,
            () -> repositoryData.addSnapshot(new SnapshotId(snapshotId.getName(), snapshotId.getUUID()), Collections.emptyList()));
        // test that adding a snapshot and its indices works
        SnapshotId newSnapshot = new SnapshotId(randomAsciiOfLength(7), UUIDs.randomBase64UUID());
        List<IndexId> indices = new ArrayList<>();
        Set<IndexId> newIndices = new HashSet<>();
        int numNew = randomIntBetween(1, 10);
        for (int i = 0; i < numNew; i++) {
            IndexId indexId = new IndexId(randomAsciiOfLength(7), UUIDs.randomBase64UUID());
            newIndices.add(indexId);
            indices.add(indexId);
        }
        int numOld = randomIntBetween(1, indexIdMap.size());
        List<String> indexNames = new ArrayList<>(indexIdMap.keySet());
        for (int i = 0; i < numOld; i++) {
            indices.add(indexIdMap.get(indexNames.get(i)));
        }
        RepositoryData newRepoData = repositoryData.addSnapshot(newSnapshot, indices);
        // verify that the new repository data has the new snapshot and its indices
        assertTrue(newRepoData.getSnapshotIds().contains(newSnapshot));
        for (IndexId indexId : indices) {
            Set<SnapshotId> snapshotIds = newRepoData.getSnapshots(indexId);
            assertTrue(snapshotIds.contains(newSnapshot));
            if (newIndices.contains(indexId)) {
                assertEquals(snapshotIds.size(), 1); // if it was a new index, only the new snapshot should be in its set
            }
        }
    }

    public void testInitIndices() {
        final int numSnapshots = randomIntBetween(1, 30);
        final List<SnapshotId> snapshotIds = new ArrayList<>(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            snapshotIds.add(new SnapshotId(randomAsciiOfLength(8), UUIDs.randomBase64UUID()));
        }
        RepositoryData repositoryData = new RepositoryData(snapshotIds, Collections.emptyMap());
        // test that initializing indices works
        Map<IndexId, Set<SnapshotId>> indices = randomIndices(snapshotIds);
        RepositoryData newRepoData = repositoryData.initIndices(indices);
        assertEquals(repositoryData.getSnapshotIds(), newRepoData.getSnapshotIds());
        for (IndexId indexId : indices.keySet()) {
            assertEquals(indices.get(indexId), newRepoData.getSnapshots(indexId));
        }
    }

    public void testRemoveSnapshot() {
        RepositoryData repositoryData = generateRandomRepoData();
        List<SnapshotId> snapshotIds = new ArrayList<>(repositoryData.getSnapshotIds());
        assertThat(snapshotIds.size(), greaterThan(0));
        SnapshotId removedSnapshotId = snapshotIds.remove(randomIntBetween(0, snapshotIds.size() - 1));
        RepositoryData newRepositoryData = repositoryData.removeSnapshot(removedSnapshotId);
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
        String notInRepoData = randomAsciiOfLength(5);
        assertFalse(indexName.contains(notInRepoData));
        assertEquals(new IndexId(notInRepoData, notInRepoData), repositoryData.resolveIndexId(notInRepoData));
    }

    public static RepositoryData generateRandomRepoData() {
        return generateRandomRepoData(new ArrayList<>());
    }

    public static RepositoryData generateRandomRepoData(final List<SnapshotId> origSnapshotIds) {
        List<SnapshotId> snapshotIds = randomSnapshots(origSnapshotIds);
        return new RepositoryData(snapshotIds, randomIndices(snapshotIds));
    }

    private static List<SnapshotId> randomSnapshots(final List<SnapshotId> origSnapshotIds) {
        final int numSnapshots = randomIntBetween(1, 30);
        final List<SnapshotId> snapshotIds = new ArrayList<>(origSnapshotIds);
        for (int i = 0; i < numSnapshots; i++) {
            snapshotIds.add(new SnapshotId(randomAsciiOfLength(8), UUIDs.randomBase64UUID()));
        }
        return snapshotIds;
    }

    private static Map<IndexId, Set<SnapshotId>> randomIndices(final List<SnapshotId> snapshotIds) {
        final int totalSnapshots = snapshotIds.size();
        final int numIndices = randomIntBetween(1, 30);
        final Map<IndexId, Set<SnapshotId>> indices = new HashMap<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            final IndexId indexId = new IndexId(randomAsciiOfLength(8), UUIDs.randomBase64UUID());
            final Set<SnapshotId> indexSnapshots = new LinkedHashSet<>();
            final int numIndicesForSnapshot = randomIntBetween(1, numIndices);
            for (int j = 0; j < numIndicesForSnapshot; j++) {
                indexSnapshots.add(snapshotIds.get(randomIntBetween(0, totalSnapshots - 1)));
            }
            indices.put(indexId, indexSnapshots);
        }
        return indices;
    }
}
