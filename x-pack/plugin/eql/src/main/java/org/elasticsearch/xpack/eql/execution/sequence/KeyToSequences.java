/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/** Dedicated collection for mapping a key to a list of sequences */
/** The list represents the sequence for each stage (based on its index) and is fixed in size */

class KeyToSequences {

    private final int listSize;
    /** for each key, associate the frame per state (determined by index) */
    private final Map<SequenceKey, SequenceGroup[]> keyToSequences;
    private final Map<SequenceKey, UntilGroup> keyToUntil;

    KeyToSequences(int listSize) {
        this.listSize = listSize;
        this.keyToSequences = new LinkedHashMap<>();
        this.keyToUntil = new LinkedHashMap<>();
    }

    private SequenceGroup[] groups(SequenceKey key) {
        return keyToSequences.computeIfAbsent(key, k -> new SequenceGroup[listSize]);
    }

    SequenceGroup groupIfPresent(int stage, SequenceKey key) {
        SequenceGroup[] groups = keyToSequences.get(key);
        return groups == null ? null : groups[stage];
    }

    UntilGroup untilIfPresent(SequenceKey key) {
        return keyToUntil.get(key);
    }

    void add(int stage, Sequence sequence) {
        SequenceKey key = sequence.key();
        SequenceGroup[] groups = groups(key);
        // create the group on demand
        if (groups[stage] == null) {
            groups[stage] = new SequenceGroup(key);
        }
        groups[stage].add(sequence);
    }

    void resetGroupInsertPosition() {
        for (SequenceGroup[] groups : keyToSequences.values()) {
            for (SequenceGroup group : groups) {
                if (group != null) {
                    group.resetInsertPosition();
                }
            }
        }
    }

    void resetUntilInsertPosition() {
        for (UntilGroup until : keyToUntil.values()) {
            if (until != null) {
                until.resetInsertPosition();
            }
        }
    }

    void until(Iterable<KeyAndOrdinal> until) {
        for (KeyAndOrdinal keyAndOrdinal : until) {
            // ignore unknown keys
            SequenceKey key = keyAndOrdinal.key();
            if (keyToSequences.containsKey(key)) {
                UntilGroup group = keyToUntil.computeIfAbsent(key, UntilGroup::new);
                group.add(keyAndOrdinal);
            }
        }
    }

    void remove(int stage, SequenceGroup group) {
        SequenceKey key = group.key();
        SequenceGroup[] groups = keyToSequences.get(key);
        groups[stage] = null;
        // clean-up the key if all groups are empty
        boolean shouldRemoveKey = true;
        for (SequenceGroup gp : groups) {
            if (gp != null && gp.isEmpty() == false) {
                shouldRemoveKey = false;
                break;
            }
        }
        if (shouldRemoveKey) {
            keyToSequences.remove(key);
        }
    }

    void dropUntil() {
        // clean-up all candidates that occur before until
        for (Entry<SequenceKey, UntilGroup> entry : keyToUntil.entrySet()) {
            SequenceGroup[] groups = keyToSequences.get(entry.getKey());
            if (groups != null) {
                for (Ordinal o : entry.getValue()) {
                    for (SequenceGroup group : groups) {
                        if (group != null) {
                            group.trimBefore(o);
                        }
                    }
                }
            }
        }

        keyToUntil.clear();
    }

    public void clear() {
        keyToSequences.clear();
        keyToUntil.clear();
    }

    int numberOfKeys() {
        return keyToSequences.size();
    }

    @Override
    public String toString() {
        return LoggerMessageFormat.format(null, "Keys=[{}], Until=[{}]", keyToSequences.size(), keyToUntil.size());
    }
}