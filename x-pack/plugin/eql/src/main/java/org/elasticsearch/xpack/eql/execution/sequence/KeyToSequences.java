/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dedicated collection for mapping a key to a list of sequences
 * The list represents the sequence for each stage (based on its index) and is fixed in size
 */
class KeyToSequences {

    /**
     * Utility class holding the sequencegroup/until tuple that also handles
     * lazy initialization.
     */
    private class SequenceEntry {

        private final SequenceGroup[] groups;
        // created lazily
        private UntilGroup until;

        SequenceEntry(int stages) {
            this.groups = new SequenceGroup[stages];
        }

        void add(int stage, Sequence sequence) {
            // create the group on demand
            if (groups[stage] == null) {
                groups[stage] = new SequenceGroup();
            }
            groups[stage].add(sequence);
        }

        public void remove(int stage) {
            groups[stage] = null;
        }

        void until(Ordinal ordinal) {
            if (until == null) {
                until = new UntilGroup();
            }
            until.add(ordinal);
        }
    }

    private final int listSize;
    /** for each key, associate the frame per state (determined by index) */
    private final Map<SequenceKey, SequenceEntry> keyToSequences;

    KeyToSequences(int listSize) {
        this.listSize = listSize;
        this.keyToSequences = new LinkedHashMap<>();
    }

    SequenceGroup groupIfPresent(int stage, SequenceKey key) {
        SequenceEntry sequenceEntry = keyToSequences.get(key);
        return sequenceEntry == null ? null : sequenceEntry.groups[stage];
    }

    UntilGroup untilIfPresent(SequenceKey key) {
        SequenceEntry sequenceEntry = keyToSequences.get(key);
        return sequenceEntry == null ? null : sequenceEntry.until;
    }

    void add(int stage, Sequence sequence) {
        SequenceKey key = sequence.key();
        SequenceEntry info = keyToSequences.computeIfAbsent(key, k -> new SequenceEntry(listSize));
        info.add(stage, sequence);
    }

    void until(Iterable<KeyAndOrdinal> until) {
        for (KeyAndOrdinal keyAndOrdinal : until) {
            // ignore unknown keys
            SequenceKey key = keyAndOrdinal.key();
            SequenceEntry sequenceEntry = keyToSequences.get(key);
            if (sequenceEntry != null) {
                sequenceEntry.until(keyAndOrdinal.ordinal);
            }
        }
    }

    void remove(int stage, SequenceKey key) {
        SequenceEntry info = keyToSequences.get(key);
        info.remove(stage);
    }

    /**
     * Remove all matches expect the latest.
     */
    void trimToTail() {
        for (Iterator<SequenceEntry> it = keyToSequences.values().iterator(); it.hasNext(); ) {
            SequenceEntry seqs =  it.next();
            // first remove the sequences
            // and remember the last item from the first
            // initialized stage to be used with until
            Sequence firstTail = null;
            for (SequenceGroup group : seqs.groups) {
                if (group != null) {
                    Sequence sequence = group.trimToLast();
                    if (firstTail == null) {
                        firstTail = sequence;
                    }
                }
            }
            // there are no sequences on any stage for this key, drop it
            if (firstTail == null) {
                it.remove();
            } else {
                // drop any possible UNTIL that occurs before the last tail
                UntilGroup until = seqs.until;
                if (until != null) {
                    until.trimBefore(firstTail.ordinal());
                }
            }
        }
    }

    public void clear() {
        keyToSequences.clear();
    }

    @Override
    public String toString() {
        return LoggerMessageFormat.format(null, "Keys=[{}]", keyToSequences.size());
    }
}
