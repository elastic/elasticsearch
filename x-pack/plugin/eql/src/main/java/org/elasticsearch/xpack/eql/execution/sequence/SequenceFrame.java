/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.collect.Tuple;

import java.util.LinkedList;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/** List of sequences (typically in a stage) used for finding continuous events within a time-frame */
public class SequenceFrame {

    // NB: since the size varies significantly, use a LinkedList
    // Considering the order it might make sense to use a B-Tree+ for faster lookups which should work well with
    // timestamp compression (whose range is known for the current frame).
    private final List<Sequence> sequences = new LinkedList<>();

    // time frame being/end
    private long tBegin = Long.MAX_VALUE, tEnd = Long.MIN_VALUE;
    private long min = tBegin, max = tEnd;

    public void add(Sequence sequence) {
        sequences.add(sequence);
        long ts = sequence.currentTimestamp();
        if (min > ts) {
            min = ts;
        }
        if (max < ts) {
            max = ts;
        }
    }

    public void setTimeFrame(long begin, long end) {
        if (tBegin > begin) {
            tBegin = begin;
        }
        if (tEnd < end) {
            tEnd = end;
        }
    }

    /**
     * Returns the latest Sequence from the group that has its timestamp
     * less than the given argument alongside its position in the list.
     */
    public Tuple<Sequence, Integer> before(long timestamp) {
        Sequence matchSeq = null;
        int matchPos = -1;
        int position = -1;
        for (Sequence sequence : sequences) {
            position++;
            if (sequence.currentTimestamp() < timestamp) {
                matchSeq = sequence;
                matchPos = position;
            } else {
                break;
            }
        }
        return matchSeq != null ? new Tuple<>(matchSeq, matchPos) : null;
    }

    /**
     * Returns the first Sequence from the group that has its timestamp
     * greater than the given argument alongside its position in the list.
     */
    public Tuple<Sequence, Integer> after(long timestamp) {
        Sequence match = null;
        int position = -1;
        for (Sequence sequence : sequences) {
            position++;
            if (sequence.currentTimestamp() > timestamp) {
                match = sequence;
            } else {
                break;
            }
        }
        return match != null ? new Tuple<>(match, position) : null;
    }

    public boolean isEmpty() {
        return sequences.isEmpty();
    }

    public void trim(int position) {
        sequences.subList(0, position).clear();
        // update min time
        if (sequences.isEmpty() == false) {
            min = sequences.get(0).currentTimestamp();
        } else {
            min = Long.MAX_VALUE;
        }
    }

    public List<Sequence> sequences() {
        return sequences;
    }

    @Override
    public String toString() {
        return format(null, "[{}-{}]({} seqs)", tBegin, tEnd, sequences.size());
    }
}