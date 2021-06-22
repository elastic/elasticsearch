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
class KeyToSequences implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(KeyToSequences.class);

    /**
     * Utility class holding the sequencegroup/until tuple that also handles
     * lazy initialization.
     */
    private static class SequenceEntry implements Accountable {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SequenceEntry.class);

        private final SequenceGroup[] groups;
        // created lazily
        private UntilGroup until;

        SequenceEntry(int stages) {
            this.groups = new SequenceGroup[stages];
        }

        long add(int stage, Sequence sequence) {
            // create the group on demand
            if (groups[stage] == null) {
                groups[stage] = new SequenceGroup();
            }
            groups[stage].add(sequence);
            return groups[stage].ramBytesUsed();
        }

        public long remove(int stage) {
            long ramBytesUsed = groups[stage].ramBytesUsed();
            groups[stage] = null;
            return ramBytesUsed;
        }

        long until(Ordinal ordinal) {
            if (until == null) {
                until = new UntilGroup();
            }
            until.add(ordinal);
            return until.ramBytesUsed();
        }

        @Override
        public long ramBytesUsed() {
            long size = SHALLOW_SIZE;
            if (until != null) {
                size += until.ramBytesUsed();
            }
            for (SequenceGroup sg : groups) {
                if (sg != null) {
                    size += sg.ramBytesUsed();
                }
            }
            size += RamUsageEstimator.shallowSizeOf(groups);
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

    long add(int stage, Sequence sequence) {
        long ramBytesUsed = 0;
        SequenceKey key = sequence.key();
        SequenceEntry info = keyToSequences.get(key);
        if (info == null) {
            info = new SequenceEntry(listSize);
            keyToSequences.put(key, info);
            ramBytesUsed += info.ramBytesUsed();
        }
        ramBytesUsed += info.add(stage, sequence);
        return ramBytesUsed;
    }

    long until(Iterable<KeyAndOrdinal> until) {
        long ramBytesUsed = 0;
        for (KeyAndOrdinal keyAndOrdinal : until) {
            // ignore unknown keys
            SequenceKey key = keyAndOrdinal.key();
            SequenceEntry sequenceEntry = keyToSequences.get(key);
            if (sequenceEntry != null) {
                ramBytesUsed += sequenceEntry.until(keyAndOrdinal.ordinal);
            }
        }
        return ramBytesUsed;
    }

    long remove(int stage, SequenceKey key) {
        SequenceEntry info = keyToSequences.get(key);
        return info.remove(stage);
    }

    /**
     * Remove all matches except the latest occurring _before_ the given ordinal.
     */
    void trimToTail(Ordinal ordinal) {
        for (Iterator<SequenceEntry> it = keyToSequences.values().iterator(); it.hasNext(); ) {
            SequenceEntry seqs =  it.next();
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
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.primitiveSizes.get(int.class);
        size += RamUsageEstimator.sizeOfMap(keyToSequences);
        return size;
    }

    @Override
    public String toString() {
        return LoggerMessageFormat.format(null, "Keys=[{}]", keyToSequences.size());
    }
}
