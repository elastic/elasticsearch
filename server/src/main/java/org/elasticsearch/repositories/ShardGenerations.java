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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

final class ShardGenerations implements ToXContent {

    private final Map<IndexId, String[]> shardGenerations;

    ShardGenerations(Map<IndexId, String[]> shardGenerations) {
        this.shardGenerations = shardGenerations;
    }

    /**
     * Computes the obsolete shard index generations that can be deleted if this instance was written to the repository.
     *
     * @param previous Previous {@code ShardGenerations}
     * @return Map of obsolete shard index generations
     */
     Map<IndexId, Map<Integer, String>> obsoleteShardGenerations(ShardGenerations previous) {
        final Map<IndexId, Map<Integer, String>> result = new HashMap<>();
        previous.shardGenerations.forEach(((indexId, oldGens) -> {
            final String[] updatedGenerations = shardGenerations.get(indexId);
            final Map<Integer, String> obsoleteShardIndices = new HashMap<>();
            if (updatedGenerations != null) {
                if (oldGens.length > 0 && Arrays.equals(updatedGenerations, oldGens) == false) {
                    assert oldGens.length == updatedGenerations.length;
                    for (int i = 0; i < oldGens.length; i++) {
                        if (updatedGenerations[i] != null && oldGens[i] != null && oldGens[i].equals(updatedGenerations[i]) == false) {
                            obsoleteShardIndices.put(i, oldGens[i]);
                        }
                    }
                }
                result.put(indexId, obsoleteShardIndices);
            }
        }));
        return result;
    }

    String getShardGen(IndexId indexId, int shardId) {
        final String[] generations = shardGenerations.get(indexId);
        if (generations == null || generations.length == 0) {
            return null;
        }
        if (generations.length < shardId - 1) {
            throw new IllegalArgumentException(
                "Index [" + indexId + "] only has [" + generations.length + "] shards but requested shard [" + shardId + "]");
        }
        return generations[shardId];
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RepositoryData.SHARDS);
        for (Map.Entry<IndexId, String[]> entry : shardGenerations.entrySet()) {
            builder.array(entry.getKey().getId(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    ShardGenerations updatedGenerations(final Map<IndexId, String[]> shardGenerations) {
        final Map<IndexId, String[]> updatedGenerations = new HashMap<>(this.shardGenerations);
        shardGenerations.forEach(((indexId, updatedGens) -> {
            final String[] existing = updatedGenerations.put(indexId, updatedGens);
            if (existing != null) {
                for (int i = 0; i < updatedGens.length; ++i) {
                    if (updatedGens[i] == null) {
                        updatedGens[i] = existing[i];
                    }
                }
            }
        }));
        updatedGenerations.putAll(shardGenerations);
        assert assertShardGensUpdateConsistent(updatedGenerations);
        return new ShardGenerations(updatedGenerations);
    }

    ShardGenerations forIndices(Set<IndexId> indices) {
        final Map<IndexId, String[]> updatedGenerations = new HashMap<>(this.shardGenerations);
        for (IndexId indexId : shardGenerations.keySet()) {
            if (indices.contains(indexId) == false) {
                updatedGenerations.remove(indexId);
            }
        }
        assert assertShardGensUpdateConsistent(updatedGenerations);
        return new ShardGenerations(updatedGenerations);
    }

    private boolean assertShardGensUpdateConsistent(Map<IndexId, String[]> updated) {
        shardGenerations.forEach((indexId, gens) -> {
            final String[] newGens = updated.get(indexId);
            assert newGens == null || gens.length == 0
                || newGens.length == gens.length : "Previous " + Arrays.asList(gens) + ", updated " + Arrays.asList(newGens);
            if (newGens != null && gens.length != 0) {
                for (int i = 0; i < newGens.length; i++) {
                    assert (newGens[i] == null && gens[i] != null) == false;
                }
            }
        });
        return true;
    }
}
