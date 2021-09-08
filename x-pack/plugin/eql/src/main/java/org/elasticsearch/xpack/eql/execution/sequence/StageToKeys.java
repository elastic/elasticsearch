/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

import static java.util.Collections.emptySet;

/** Dedicated collection for mapping a stage (represented by the index collection) to a set of keys */
class StageToKeys implements Accountable {

    private final List<Set<SequenceKey>> stageToKey;

    @SuppressWarnings(value = { "unchecked", "rawtypes" })
    StageToKeys(int stages) {
        // use asList to create an immutable list already initialized to null
        this.stageToKey = Arrays.asList(new Set[stages]);
    }

    void add(int stage, SequenceKey key) {
        Set<SequenceKey> set = stageToKey.get(stage);
        if (set == null) {
            // TODO: could we use an allocation strategy?
            set = new LinkedHashSet<>();
            stageToKey.set(stage, set);
        }
        set.add(key);
    }

    void remove(int stage, SequenceKey key) {
        Set<SequenceKey> set = stageToKey.get(stage);
        if (set != null) {
            set.remove(key);
        }
    }

    boolean isEmpty(int stage) {
        Set<SequenceKey> set = stageToKey.get(stage);
        return set == null || set.isEmpty();
    }

    Set<SequenceKey> keys(int stage) {
        Set<SequenceKey> set = stageToKey.get(stage);
        return set == null ? emptySet() : set;
    }

    Set<SequenceKey> keys() {
        Set<SequenceKey> keys = new LinkedHashSet<>();
        for (Set<SequenceKey> sequenceKeys : stageToKey) {
            if (CollectionUtils.isEmpty(sequenceKeys) == false) {
                keys.addAll(sequenceKeys);
            }
        }
        return keys;
    }

    void clear() {
        for (Set<SequenceKey> set : stageToKey) {
            if (set != null) {
                set.clear();
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.sizeOfCollection(stageToKey);
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(",", "[", "]");
        stageToKey.forEach(s -> sj.add(s != null ? "" + s.size() : "0"));
        return sj.toString();
    }
}
