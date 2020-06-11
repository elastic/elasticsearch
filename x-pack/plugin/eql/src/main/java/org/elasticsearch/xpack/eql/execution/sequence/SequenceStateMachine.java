/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.SearchHit;

import java.util.LinkedList;
import java.util.List;

/**
 * State machine that holds and manages all in-flight sequences.
 */
public class SequenceStateMachine {

    /** Current sequences for each key */
    /** Note will be multiple sequences for the same key and the same stage with different timestamps */
    private final KeyToSequences keyToSequences;
    /** Current keys on each stage */
    private final StageToKeys stageToKeys;

    /** minimum timestamp per stage */
    /** this ignores the key */
    private final long[] timestampMarkers;

    private final Comparable<Object>[] tiebreakerMarkers;
    private final boolean hasTieBreaker;

    private final int completionStage;

    /** list of completed sequences - separate to avoid polluting the other stages */
    private final List<Sequence> completed;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public SequenceStateMachine(int stages, boolean hasTiebreaker) {
        this.completionStage = stages - 1;

        this.stageToKeys = new StageToKeys(completionStage);
        this.keyToSequences = new KeyToSequences(completionStage);
        this.timestampMarkers = new long[completionStage];
        this.tiebreakerMarkers = new Comparable[completionStage];
        this.completed = new LinkedList<>();

        this.hasTieBreaker = hasTiebreaker;
    }

    public List<Sequence> completeSequences() {
        return completed;
    }

    public long getTimestampMarker(int stage) {
        return timestampMarkers[stage];
    }

    public Comparable<?> getTiebreakerMarker(int stage) {
        return tiebreakerMarkers[stage];
    }

    public void setTimestampMarker(int stage, long timestamp) {
        timestampMarkers[stage] = timestamp;
    }

    public void setTiebreakerMarker(int stage, Comparable<Object> tiebreaker) {
        tiebreakerMarkers[stage] = tiebreaker;
    }

    public Object[] getMarkers(int stage) {
        long ts = timestampMarkers[stage];
        Comparable<Object> tb = tiebreakerMarkers[stage];
        return hasTieBreaker ? new Object[] { ts, tb } : new Object[] { ts };
    }

    public void trackSequence(Sequence sequence, long tMin, long tMax) {
        SequenceKey key = sequence.key();

        stageToKeys.keys(0).add(key);
        SequenceFrame frame = keyToSequences.frame(0, key);
        frame.setTimeFrame(tMin, tMax);
        frame.add(sequence);
    }

    /**
     * Match the given hit (based on key and timestamp and potential tiebreaker) with any potential sequence from the previous
     * given stage. If that's the case, update the sequence and the rest of the references.
     */
    public boolean match(int stage, SequenceKey key, long timestamp, Comparable<Object> tiebreaker, SearchHit hit) {
        int previousStage = stage - 1;
        // check key presence to avoid creating a collection
        SequenceFrame frame = keyToSequences.frameIfPresent(previousStage, key);
        if (frame == null || frame.isEmpty()) {
            return false;
        }
        // pick the sequence with the highest timestamp lower than current match timestamp
        Tuple<Sequence, Integer> before = frame.before(timestamp, tiebreaker);
        if (before == null) {
            return false;
        }
        Sequence sequence = before.v1();
        // eliminate the match and all previous values from the frame
        frame.trim(before.v2() + 1);
        // update sequence
        sequence.putMatch(stage, hit, timestamp, tiebreaker);

        // remove the frame and keys early (as the key space is large)
        if (frame.isEmpty()) {
            stageToKeys.keys(previousStage).remove(key);
        }

        // bump the stages
        if (stage == completionStage) {
            completed.add(sequence);
        } else {
            stageToKeys.keys(stage).add(key);
            keyToSequences.frame(stage, key).add(sequence);
        }
        return true;
    }
}