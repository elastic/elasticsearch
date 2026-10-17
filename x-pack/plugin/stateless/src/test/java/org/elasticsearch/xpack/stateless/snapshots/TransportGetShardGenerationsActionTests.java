/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TransportGetShardGenerationsActionTests extends ESTestCase {

    private static final IndexId INDEX = new IndexId("index", "index-id");
    private static final IndexId OTHER_INDEX = new IndexId("other", "other-id");

    private static ShardId shardId(String index, int id) {
        return new ShardId(new Index(index, index + "-uuid"), id);
    }

    private static RepositoryData repositoryData(long generation, ShardGenerations shardGenerations) {
        final var snapshot = new SnapshotId("snap", "snap-uuid");
        return new RepositoryData(
            "uuid",
            generation,
            Map.of("snap", snapshot),
            Map.of(),
            Map.of(INDEX, List.of(snapshot), OTHER_INDEX, List.of(snapshot)),
            shardGenerations,
            IndexMetaDataGenerations.EMPTY,
            "cluster-uuid"
        );
    }

    public void testShardsOfSeveralIndicesAreAnsweredInOneResponse() {
        final var gen0 = new ShardGeneration("gen0");
        final var gen1 = new ShardGeneration("gen1");
        final var otherGen = new ShardGeneration("otherGen");
        final var data = repositoryData(
            7,
            ShardGenerations.builder().put(INDEX, 0, gen0).put(INDEX, 1, gen1).put(OTHER_INDEX, 0, otherGen).build()
        );

        final var response = TransportGetShardGenerationsAction.getResponse(
            data,
            List.of(shardId("index", 0), shardId("index", 1), shardId("other", 0))
        );
        assertThat(response.getRepositoryGeneration(), equalTo(7L));
        assertThat(
            response.getShardGenerations(),
            equalTo(
                Map.of(
                    shardId("index", 0),
                    new RepositoryShardGeneration(INDEX, gen0),
                    shardId("index", 1),
                    new RepositoryShardGeneration(INDEX, gen1),
                    shardId("other", 0),
                    new RepositoryShardGeneration(OTHER_INDEX, otherGen)
                )
            )
        );
    }

    public void testShardsTheRepositoryHasNoGenerationForAreAnsweredWithNull() {
        final var gen0 = new ShardGeneration("gen0");
        final var data = repositoryData(
            3,
            ShardGenerations.builder()
                .put(INDEX, 0, gen0)
                .put(INDEX, 1, ShardGenerations.NEW_SHARD_GEN)
                .put(INDEX, 2, ShardGenerations.DELETED_SHARD_GEN)
                .build()
        );

        final var response = TransportGetShardGenerationsAction.getResponse(
            data,
            Set.of(
                shardId("index", 0),
                shardId("index", 1), // new
                shardId("index", 2), // deleted
                shardId("index", 3), // past the end of the shards of the index
                shardId("absent", 0) // an index the repository does not have
            )
        );
        final var generations = response.getShardGenerations();
        assertThat(generations.size(), equalTo(5));
        assertThat(generations.get(shardId("index", 0)), equalTo(new RepositoryShardGeneration(INDEX, gen0)));
        for (var shardId : List.of(shardId("index", 1), shardId("index", 2), shardId("index", 3), shardId("absent", 0))) {
            assertTrue(shardId.toString(), generations.containsKey(shardId));
            assertThat(shardId.toString(), generations.get(shardId), nullValue());
        }
    }

    public void testAnEmptyRepositoryHasNoGenerations() {
        final var response = TransportGetShardGenerationsAction.getResponse(RepositoryData.EMPTY, List.of(shardId("index", 0)));
        assertThat(response.getRepositoryGeneration(), equalTo(RepositoryData.EMPTY_REPO_GEN));
        assertTrue(response.getShardGenerations().containsKey(shardId("index", 0)));
        assertThat(response.getShardGenerations().get(shardId("index", 0)), nullValue());
    }
}
