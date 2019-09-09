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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class ShardGenerations implements ToXContent {

    public static final ShardGenerations EMPTY = new ShardGenerations(Collections.emptyMap());

    private final Map<IndexId, List<String>> shardGenerations;

    ShardGenerations(Map<IndexId, List<String>> shardGenerations) {
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
            final List<String> updatedGenerations = shardGenerations.get(indexId);
            final Map<Integer, String> obsoleteShardIndices = new HashMap<>();
            if (updatedGenerations != null) {
                if (oldGens.isEmpty() == false && oldGens.equals(updatedGenerations) == false) {
                    assert oldGens.size() == updatedGenerations.size();
                    for (int i = 0; i < oldGens.size(); i++) {
                        if (updatedGenerations.get(i) != null && oldGens.get(i) != null
                            && oldGens.get(i).equals(updatedGenerations.get(i)) == false) {
                            obsoleteShardIndices.put(i, oldGens.get(i));
                        }
                    }
                }
                result.put(indexId, obsoleteShardIndices);
            }
        }));
        return result;
    }

    String getShardGen(IndexId indexId, int shardId) {
        final List<String> generations = shardGenerations.get(indexId);
        if (generations == null || generations.isEmpty()) {
            return null;
        }
        if (generations.size() < shardId - 1) {
            throw new IllegalArgumentException(
                "Index [" + indexId + "] only has [" + generations.size() + "] shards but requested shard [" + shardId + "]");
        }
        return generations.get(shardId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RepositoryData.SHARDS);
        for (Map.Entry<IndexId, List<String>> entry : shardGenerations.entrySet()) {
            builder.array(entry.getKey().getId(), entry.getValue().toArray(Strings.EMPTY_ARRAY));
        }
        builder.endObject();
        return builder;
    }

    ShardGenerations updatedGenerations(final Map<IndexId, String[]> shardGenerations) {
        final Map<IndexId, List<String>> updatedGenerations = new HashMap<>(this.shardGenerations);
        shardGenerations.forEach(((indexId, updatedGens) -> {
            final List<String> existing = updatedGenerations.put(indexId, Arrays.asList(updatedGens));
            if (existing != null) {
                for (int i = 0; i < updatedGens.length; ++i) {
                    if (updatedGens[i] == null) {
                        updatedGens[i] = existing.get(i);
                    }
                }
            }
        }));
        assert assertShardGensUpdateConsistent(updatedGenerations);
        return new ShardGenerations(updatedGenerations);
    }

    ShardGenerations forIndices(Set<IndexId> indices) {
        final Map<IndexId, List<String>> updatedGenerations = new HashMap<>(this.shardGenerations);
        for (IndexId indexId : shardGenerations.keySet()) {
            if (indices.contains(indexId) == false) {
                updatedGenerations.remove(indexId);
            }
        }
        assert assertShardGensUpdateConsistent(updatedGenerations);
        return new ShardGenerations(updatedGenerations);
    }

    private boolean assertShardGensUpdateConsistent(Map<IndexId, List<String>> updated) {
        shardGenerations.forEach((indexId, gens) -> {
            final List<String> newGens = updated.get(indexId);
            assert newGens == null || gens.size() == 0
                || newGens.size() == gens.size() : "Previous " + gens + ", updated " + newGens;
            if (newGens != null && gens.size() != 0) {
                for (int i = 0; i < newGens.size(); i++) {
                    assert (newGens.get(i) == null && gens.get(i) != null) == false;
                }
            }
        });
        return true;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ShardGenerations that = (ShardGenerations) o;
        return shardGenerations.equals(that.shardGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardGenerations);
    }
}
