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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a {@code Map<String, String>} that implements AbstractDiffable so it
 * can be used for cluster state purposes
 */
public class DiffableStringMap extends AbstractMap<String, String> implements Diffable<DiffableStringMap> {

    public static final DiffableStringMap EMPTY = new DiffableStringMap(Collections.emptyMap());

    private final Map<String, String> innerMap;

    DiffableStringMap(final Map<String, String> map) {
        this.innerMap = Collections.unmodifiableMap(map);
    }

    @SuppressWarnings("unchecked")
    DiffableStringMap(final StreamInput in) throws IOException {
        this((Map<String, String>) (Map) in.readMap());
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return innerMap.entrySet();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap((Map<String, Object>) (Map) innerMap);
    }

    @Override
    public Diff<DiffableStringMap> diff(DiffableStringMap previousState) {
        return new DiffableStringMapDiff(previousState, this);
    }

    public static Diff<DiffableStringMap> readDiffFrom(StreamInput in) throws IOException {
        return new DiffableStringMapDiff(in);
    }

    /**
     * Represents differences between two DiffableStringMaps.
     */
    public static class DiffableStringMapDiff implements Diff<DiffableStringMap> {

        public static final DiffableStringMapDiff EMPTY = new DiffableStringMapDiff(DiffableStringMap.EMPTY, DiffableStringMap.EMPTY);

        private final List<String> deletes;
        private final Map<String, String> upserts; // diffs also become upserts

        private DiffableStringMapDiff(DiffableStringMap before, DiffableStringMap after) {
            final List<String> tempDeletes = new ArrayList<>();
            final Map<String, String> tempUpserts = new HashMap<>();
            for (String key : before.keySet()) {
                if (after.containsKey(key) == false) {
                    tempDeletes.add(key);
                }
            }

            for (Map.Entry<String, String> partIter : after.entrySet()) {
                String beforePart = before.get(partIter.getKey());
                if (beforePart == null) {
                    tempUpserts.put(partIter.getKey(), partIter.getValue());
                } else if (partIter.getValue().equals(beforePart) == false) {
                    tempUpserts.put(partIter.getKey(), partIter.getValue());
                }
            }
            deletes = tempDeletes;
            upserts = tempUpserts;
        }

        private DiffableStringMapDiff(StreamInput in) throws IOException {
            deletes = new ArrayList<>();
            upserts = new HashMap<>();
            int deletesCount = in.readVInt();
            for (int i = 0; i < deletesCount; i++) {
                deletes.add(in.readString());
            }
            int upsertsCount = in.readVInt();
            for (int i = 0; i < upsertsCount; i++) {
                String key = in.readString();
                String newValue = in.readString();
                upserts.put(key, newValue);
            }
        }

        public List<String> getDeletes() {
            return deletes;
        }

        public Map<String, Diff<String>> getDiffs() {
            return Collections.emptyMap();
        }

        public Map<String, String> getUpserts() {
            return upserts;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(deletes.size());
            for (String delete : deletes) {
                out.writeString(delete);
            }
            out.writeVInt(upserts.size());
            for (Map.Entry<String, String> entry : upserts.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
        }

        @Override
        public DiffableStringMap apply(DiffableStringMap part) {
            Map<String, String> builder = new HashMap<>(part.innerMap);
            List<String> deletes = getDeletes();
            for (String delete : deletes) {
                builder.remove(delete);
            }
            assert getDiffs().size() == 0 : "there should never be diffs for DiffableStringMap";

            for (Map.Entry<String, String> upsert : upserts.entrySet()) {
                builder.put(upsert.getKey(), upsert.getValue());
            }
            return new DiffableStringMap(builder);
        }
    }
}
