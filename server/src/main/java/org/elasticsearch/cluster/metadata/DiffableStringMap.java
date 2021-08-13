/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    @SuppressWarnings("unchecked")
    public static DiffableStringMap readFrom(StreamInput in) throws IOException {
        final Map<String, String> map = (Map) in.readMap();
        return map.isEmpty() ? EMPTY : new DiffableStringMap(map);
    }

    DiffableStringMap(final Map<String, String> map) {
        this.innerMap = Collections.unmodifiableMap(map);
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
            deletes = in.readStringList();
            upserts = in.readMap(StreamInput::readString, StreamInput::readString);
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
            out.writeStringCollection(deletes);
            out.writeMap(upserts, StreamOutput::writeString, StreamOutput::writeString);
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
