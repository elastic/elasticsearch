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
    public String get(Object key) {
        return innerMap.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return innerMap.containsKey(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap((Map<String, Object>) (Map) innerMap);
    }

    @Override
    public Diff<DiffableStringMap> diff(DiffableStringMap previousState) {
        if (equals(previousState)) {
            return DiffableStringMapDiff.EMPTY;
        }
        final List<String> tempDeletes = new ArrayList<>();
        for (String key : previousState.keySet()) {
            if (containsKey(key) == false) {
                tempDeletes.add(key);
            }
        }

        final Map<String, String> tempUpserts = new HashMap<>();
        for (Map.Entry<String, String> partIter : entrySet()) {
            String beforePart = previousState.get(partIter.getKey());
            if (beforePart == null) {
                tempUpserts.put(partIter.getKey(), partIter.getValue());
            } else if (partIter.getValue().equals(beforePart) == false) {
                tempUpserts.put(partIter.getKey(), partIter.getValue());
            }
        }
        return getDiff(tempDeletes, tempUpserts);
    }

    public static Diff<DiffableStringMap> readDiffFrom(StreamInput in) throws IOException {
        final List<String> deletes = in.readStringList();
        final Map<String, String> upserts = in.readMap(StreamInput::readString, StreamInput::readString);
        return getDiff(deletes, upserts);
    }

    private static Diff<DiffableStringMap> getDiff(List<String> deletes, Map<String, String> upserts) {
        if (deletes.isEmpty() && upserts.isEmpty()) {
            return DiffableStringMapDiff.EMPTY;
        }
        return new DiffableStringMapDiff(deletes, upserts);
    }

    /**
     * Represents differences between two DiffableStringMaps.
     */
    public static class DiffableStringMapDiff implements Diff<DiffableStringMap> {

        public static final DiffableStringMapDiff EMPTY = new DiffableStringMapDiff(List.of(), Map.of()) {
            @Override
            public DiffableStringMap apply(DiffableStringMap part) {
                return part;
            }
        };

        private final List<String> deletes;
        private final Map<String, String> upserts; // diffs also become upserts

        private DiffableStringMapDiff(List<String> deletes, Map<String, String> upserts) {
            this.deletes = deletes;
            this.upserts = upserts;
        }

        public List<String> getDeletes() {
            return deletes;
        }

        public static Map<String, Diff<String>> getDiffs() {
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

            builder.putAll(upserts);
            return new DiffableStringMap(builder);
        }
    }
}
