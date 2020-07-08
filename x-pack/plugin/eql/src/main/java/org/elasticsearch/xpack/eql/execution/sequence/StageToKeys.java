/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

/** Dedicated collection for mapping a stage (represented by the index collection) to a set of keys */
class StageToKeys {

    private final List<Set<SequenceKey>> stageToKey;

    @SuppressWarnings(value = { "unchecked", "rawtypes" })
    StageToKeys(int stages) {
        // use asList to create an immutable list already initialized to null
        this.stageToKey = Arrays.asList(new Set[stages]);
    }

    Set<SequenceKey> keys(int stage) {
        Set<SequenceKey> set = stageToKey.get(stage);
        if (set == null) {
            // TODO: could we use an allocation strategy?
            set = new LinkedHashSet<>();
            stageToKey.set(stage, set);
        }
        return set;
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(",", "[", "]");
        stageToKey.forEach(s -> sj.add(s != null ? "" + s.size() : "0"));
        return sj.toString();
    }
}
