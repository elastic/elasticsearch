/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.collect.Tuple;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/** List of sequences (typically in a stage) used for finding continuous events within a time-frame */
public class SequenceFrame {

    // NB: since the size varies significantly, use a LinkedList
    // Considering the order it might make sense to use a B-Tree+ for faster lookups which should work well with
    // timestamp compression (whose range is known for the current frame).
    private final List<Sequence> sequences = new LinkedList<>();

    private Ordinal start, stop;

    public void add(Sequence sequence) {
        sequences.add(sequence);
        Ordinal ordinal = sequence.ordinal();
        if (start == null) {
            start = ordinal;
            stop = ordinal;
        } else {
            if (start.compareTo(ordinal) > 0) {
                start = ordinal;
            }
            if (stop.compareTo(ordinal) < 0) {
                stop = ordinal;
            }
        }
    }

    /**
     * Returns the latest Sequence from the group that has its timestamp
     * less than the given argument alongside its position in the list.
     */
    public Tuple<Sequence, Integer> before(Ordinal ordinal) {
        return find(o -> o.compareTo(ordinal) < 0);
    }

    /**
     * Returns the first Sequence from the group that has its timestamp
     * greater than the given argument alongside its position in the list.
     */
    public Tuple<Sequence, Integer> after(Ordinal ordinal) {
        return find(o -> o.compareTo(ordinal) > 0);
    }

    private Tuple<Sequence, Integer> find(Predicate<Ordinal> predicate) {
        Sequence matchSeq = null;
        int matchPos = -1;
        int position = -1;
        for (Sequence sequence : sequences) {
            position++;
            if (predicate.test(sequence.ordinal())) {
                matchSeq = sequence;
                matchPos = position;
            } else {
                break;
            }
        }
        return matchSeq != null ? new Tuple<>(matchSeq, matchPos) : null;
    }

    public boolean isEmpty() {
        return sequences.isEmpty();
    }

    public void trim(int position) {
        sequences.subList(0, position).clear();

        // update min time
        if (sequences.isEmpty() == false) {
            start = sequences.get(0).ordinal();
        } else {
            stop = null;
        }
    }

    public List<Sequence> sequences() {
        return sequences;
    }

    @Override
    public String toString() {
        return format(null, "[{}-{}]({} seqs)", start, stop, sequences.size());
    }
}