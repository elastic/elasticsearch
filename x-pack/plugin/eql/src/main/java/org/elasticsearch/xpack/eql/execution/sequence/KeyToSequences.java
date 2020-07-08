/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import java.util.LinkedHashMap;
import java.util.Map;

/** Dedicated collection for mapping a key to a list of sequences */
/** The list represents the sequence for each stage (based on its index) and is fixed in size */

class KeyToSequences {

    private final int listSize;
    /** for each key, associate the frame per state (determined by index) */
    private final Map<SequenceKey, SequenceGroup[]> keyToSequences;

    KeyToSequences(int listSize) {
        this.listSize = listSize;
        this.keyToSequences = new LinkedHashMap<>();
    }

    private SequenceGroup[] group(SequenceKey key) {
        SequenceGroup[] groups = keyToSequences.get(key);
        if (groups == null) {
            groups = new SequenceGroup[listSize];
            keyToSequences.put(key, groups);
        }
        return groups;
    }

    SequenceGroup groupIfPresent(int stage, SequenceKey key) {
        SequenceGroup[] groups = keyToSequences.get(key);
        return groups == null ? null : groups[stage];
    }

    void add(int stage, Sequence sequence) {
        SequenceKey key = sequence.key();
        SequenceGroup[] groups = group(key);
        // create the group on demand
        if (groups[stage] == null) {
            groups[stage] = new SequenceGroup(key);
        }
        groups[stage].add(sequence);
    }

    int numberOfKeys() {
        return keyToSequences.size();
    }
}
