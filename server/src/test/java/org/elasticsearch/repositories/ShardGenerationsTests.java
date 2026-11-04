/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * The names of an index that was renamed between snapshots share the repository id, so they share the shard paths and the shard
 * generations. These tests cover how {@link ShardGenerations} and its builder find the names of an id.
 */
public class ShardGenerationsTests extends ESTestCase {

    private static IndexId indexId(String name, String id) {
        return new IndexId(name, id);
    }

    public void testBuilderSharesGenerationsBetweenNamesOfOneId() {
        final String id = UUIDs.randomBase64UUID(random());
        final IndexId oldName = indexId("old", id);
        final IndexId newName = indexId("new", id);
        final IndexId other = indexId("other", UUIDs.randomBase64UUID(random()));
        final ShardGeneration first = ShardGeneration.newGeneration(random());
        final ShardGeneration second = ShardGeneration.newGeneration(random());
        final ShardGeneration third = ShardGeneration.newGeneration(random());
        final ShardGeneration unrelated = ShardGeneration.newGeneration(random());

        final ShardGenerations generations = ShardGenerations.builder()
            .put(oldName, 0, first)
            .put(other, 0, unrelated)
            .put(newName, 0, second)
            .put(oldName, 1, third)
            .build();

        // an update under either name is seen under both, and a different id is not touched
        assertThat(generations.getShardGen(oldName, 0), equalTo(second));
        assertThat(generations.getShardGen(newName, 0), equalTo(second));
        assertThat(generations.getShardGen(oldName, 1), equalTo(third));
        assertThat(generations.getShardGen(newName, 1), equalTo(third));
        assertThat(generations.getShardGen(other, 0), equalTo(unrelated));
        assertThat(generations.getGens(other), contains(unrelated));
    }

    /**
     * A name that is dropped from the builder before a later update must not come back, and an update under its sibling must not
     * fail on it.
     */
    public void testBuilderSkipsNamesThatWerePruned() {
        final String id = UUIDs.randomBase64UUID(random());
        final IndexId oldName = indexId("old", id);
        final IndexId newName = indexId("new", id);
        final ShardGeneration first = ShardGeneration.newGeneration(random());
        final ShardGeneration second = ShardGeneration.newGeneration(random());

        final ShardGenerations.Builder builder = ShardGenerations.builder().put(oldName, 0, first).put(newName, 0, first);
        builder.retainIndicesAndPruneDeletes(Set.of(newName));
        builder.put(newName, 1, second);
        final ShardGenerations generations = builder.build();

        assertThat(generations.indices(), contains(newName));
        assertThat(generations.getGens(newName), contains(first, second));
    }

    public void testLookupFallsBackToAnotherNameOfTheId() {
        final String id = UUIDs.randomBase64UUID(random());
        final IndexId tracked = indexId("tracked", id);
        final ShardGeneration generation = ShardGeneration.newGeneration(random());
        final ShardGenerations generations = ShardGenerations.builder().put(tracked, 0, generation).build();

        final IndexId anotherName = indexId("another-name", id);
        assertThat(generations.getShardGen(anotherName, 0), equalTo(generation));
        assertThat(generations.getGens(anotherName), contains(generation));
        assertThat(generations.getShardGen(anotherName, 1), equalTo(null));

        final IndexId unknown = indexId("unknown", UUIDs.randomBase64UUID(random()));
        assertThat(generations.getShardGen(unknown, 0), equalTo(null));
        assertThat(generations.getGens(unknown), empty());
        assertThat(ShardGenerations.EMPTY.getGens(unknown), empty());
    }

    /**
     * The lookups by id are answered from a map built once. For many indices, some of which share an id, they must give what a scan
     * of the tracked indices gives.
     */
    public void testLookupsAgreeWithAScan() {
        final ShardGenerations.Builder builder = ShardGenerations.builder();
        final List<IndexId> tracked = new ArrayList<>();
        final List<IndexId> otherNames = new ArrayList<>();
        final int indices = randomIntBetween(1, 200);
        for (int i = 0; i < indices; i++) {
            final IndexId indexId = indexId("index-" + i, UUIDs.randomBase64UUID(random()));
            tracked.add(indexId);
            final int shards = randomIntBetween(1, 3);
            for (int shard = 0; shard < shards; shard++) {
                builder.put(indexId, shard, ShardGeneration.newGeneration(random()));
            }
            if (randomIntBetween(0, 3) == 0) {
                // a name for the same id that the builder is never given
                otherNames.add(indexId("renamed-" + i, indexId.getId()));
            }
        }
        final ShardGenerations generations = builder.build();

        final List<IndexId> queries = new ArrayList<>(tracked);
        queries.addAll(otherNames);
        queries.add(indexId("unknown", UUIDs.randomBase64UUID(random())));
        for (IndexId query : queries) {
            final List<ShardGeneration> expected = tracked.stream()
                .filter(indexId -> indexId.getId().equals(query.getId()))
                .map(generations::getGens)
                .findFirst()
                .orElse(List.of());
            assertThat(generations.getGens(query), equalTo(expected));
            for (int shard = 0; shard < 4; shard++) {
                assertThat(generations.getShardGen(query, shard), equalTo(shard < expected.size() ? expected.get(shard) : null));
            }
        }
        assertThat(generations.indices(), containsInAnyOrder(tracked.toArray()));
    }
}
