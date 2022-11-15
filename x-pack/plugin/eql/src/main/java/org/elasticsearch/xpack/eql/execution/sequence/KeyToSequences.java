/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dedicated collection for mapping a key to a list of sequences
 * The list represents the sequence for each stage (based on its index) and is fixed in size
 */
public class KeyToSequences implements Accountable {

    /**
     * Utility class holding the sequencegroup/until tuple that also handles
     * lazy initialization.
     */
    public static class SequenceEntry implements Accountable {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SequenceEntry.class);

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

        @Override
        public long ramBytesUsed() {
            long size = SHALLOW_SIZE;
            if (until != null) {
                size += until.ramBytesUsed();
            }
            size += RamUsageEstimator.sizeOf(groups);
            return size;
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
     * Remove all matches except the latest occurring _before_ the given ordinal.
     */
    void trimToTail(Ordinal ordinal) {
        for (Iterator<SequenceEntry> it = keyToSequences.values().iterator(); it.hasNext();) {
            SequenceEntry seqs = it.next();
            // remember the last item found (will be ascending)
            // to trim unneeded until that occur before it
            Sequence firstTail = null;
            // remove any empty keys
            boolean keyIsEmpty = true;
            for (SequenceGroup group : seqs.groups) {
                if (group != null) {
                    Sequence sequence = group.trimBeforeLast(ordinal);
                    if (firstTail == null) {
                        firstTail = sequence;
                    }
                    keyIsEmpty &= group.isEmpty();
                }
            }
            // there are no sequences on any stage for this key, drop it
            if (keyIsEmpty) {
                it.remove();
            }
            if (firstTail != null) {
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
    public long ramBytesUsed() {
        return RamUsageEstimator.sizeOfMap(keyToSequences);
    }

    @Override
    public String toString() {
        return LoggerMessageFormat.format(null, "Keys=[{}]", keyToSequences.size());
    }
}
