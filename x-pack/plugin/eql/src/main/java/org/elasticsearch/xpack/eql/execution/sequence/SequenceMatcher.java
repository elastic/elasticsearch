/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.LinkedList;
import java.util.List;

/**
 * Matcher of sequences. Keeps track of on-going sequences and advancing them through each stage.
 */
public class SequenceMatcher {

    private final Logger log = LogManager.getLogger(SequenceMatcher.class);

    static class Stats {
        long seen = 0;
        long ignored = 0;
        long until = 0;
        long rejectionMaxspan = 0;
        long rejectionUntil = 0;
        
        @Override
        public String toString() {
            return LoggerMessageFormat.format(null, "Stats: Seen [{}]/Ignored [{}]/Until [{}]/Rejected {Maxspan [{}]/Until [{}]}",
                    seen,
                    ignored,
                    until,
                    rejectionMaxspan,
                    rejectionUntil);
        }

        public void clear() {
            seen = 0;
            ignored = 0;
            until = 0;
            rejectionMaxspan = 0;
            rejectionUntil = 0;
        }
    }

    /** Current sequences for each key */
    /** Note will be multiple sequences for the same key and the same stage with different timestamps */
    private final KeyToSequences keyToSequences;
    /** Current keys on each stage */
    private final StageToKeys stageToKeys;

    private final int numberOfStages;
    private final int completionStage;

    /** list of completed sequences - separate to avoid polluting the other stages */
    private final List<Sequence> completed;
    private int completedInsertPosition = 0;

    private final long maxSpanInMillis;

    private final boolean descending;

    private Limit limit;
    private boolean headLimit = false;

    private final Stats stats = new Stats();

    @SuppressWarnings("rawtypes")
    public SequenceMatcher(int stages, boolean descending, TimeValue maxSpan, Limit limit) {
        this.numberOfStages = stages;
        this.completionStage = stages - 1;

        this.descending = descending;
        this.stageToKeys = new StageToKeys(completionStage);
        this.keyToSequences = new KeyToSequences(completionStage);
        this.completed = new LinkedList<>();

        this.maxSpanInMillis = maxSpan.millis();

        // limit
        this.limit = limit;
    }

    private void trackSequence(Sequence sequence) {
        SequenceKey key = sequence.key();

        stageToKeys.add(0, key);
        keyToSequences.add(0, sequence);

        stats.seen++;
    }

    /**
     * Match hits for the given stage.
     * Returns false if the process needs to be stopped.
     */
    boolean match(int stage, Iterable<Tuple<KeyAndOrdinal, HitReference>> hits) {
        for (Tuple<KeyAndOrdinal, HitReference> tuple : hits) {
            KeyAndOrdinal ko = tuple.v1();
            HitReference hit = tuple.v2();

            if (stage == 0) {
                Sequence seq = new Sequence(ko.key, numberOfStages, ko.ordinal, hit);
                // descending queries return descending blocks of ASC data
                // to avoid sorting things during insertion,

                trackSequence(seq);
            } else {
                match(stage, ko.key, ko.ordinal, hit);

                // early skip in case of reaching the limit
                // check the last stage to avoid calling the state machine in other stages
                if (headLimit) {
                    log.trace("(Head) Limit reached {}", stats);
                    return false;
                }
            }
        }

        // check tail limit
        if (tailLimitReached()) {
            log.trace("(Tail) Limit reached {}", stats);
            return false;
        }
        log.trace("{}", stats);
        return true;
    }

    private boolean tailLimitReached() {
        return limit != null && limit.limit() < 0 && limit.absLimit() <= completed.size();
    }

    /**
     * Match the given hit (based on key and timestamp and potential tiebreaker) with any potential sequence from the previous
     * given stage. If that's the case, update the sequence and the rest of the references.
     */
    private void match(int stage, SequenceKey key, Ordinal ordinal, HitReference hit) {
        stats.seen++;
        
        int previousStage = stage - 1;
        // check key presence to avoid creating a collection
        SequenceGroup group = keyToSequences.groupIfPresent(previousStage, key);
        if (group == null || group.isEmpty()) {
            stats.ignored++;
            return;
        }

        // eliminate the match and all previous values from the group
        Sequence sequence = group.trimBefore(ordinal);
        if (sequence == null) {
            stats.ignored++;
            return;
        }

        // remove the group early (as the key space is large)
        if (group.isEmpty()) {
            keyToSequences.remove(previousStage, group);
            stageToKeys.remove(previousStage, key);
        }
        
        //
        // Conditional checks
        //

        // maxspan
        if (maxSpanInMillis > 0 && (ordinal.timestamp() - sequence.startOrdinal().timestamp() > maxSpanInMillis)) {
            stats.rejectionMaxspan++;
            return;
        }

        // until
        UntilGroup until = keyToSequences.untilIfPresent(key);
        if (until != null) {
            KeyAndOrdinal nearestUntil = until.before(ordinal);
            if (nearestUntil != null) {
                // check if until matches
                if (nearestUntil.ordinal().between(sequence.ordinal(), ordinal)) {
                    stats.rejectionUntil++;
                    return;
                }
            }
        }
        
        sequence.putMatch(stage, ordinal, hit);

        // bump the stages
        if (stage == completionStage) {
            // when dealing with descending queries
            // avoid duplicate matching (since the ASC query can return previously seen results)
            if (descending) {
                for (Sequence seen : completed) {
                    if (seen.key().equals(key) && seen.ordinal().equals(ordinal)) {
                        return;
                    }
                }
            }

            completed.add(completedInsertPosition++, sequence);
            // update the bool lazily
            // only consider positive limits / negative ones imply tail which means having to go
            // through the whole page of results before selecting the last ones
            // doing a limit early returns the 'head' not 'tail'
            headLimit = limit != null && limit.limit() > 0 && completed.size() == limit.totalLimit();
        } else {
            if (descending) {
                // when dealing with descending queries
                // avoid duplicate matching (since the ASC query can return previously seen results)
                group = keyToSequences.groupIfPresent(stage, key);
                if (group != null) {
                    for (Ordinal previous : group) {
                        if (previous.equals(ordinal)) {
                            return;
                        }
                    }
                }
            }

            stageToKeys.add(stage, key);
            keyToSequences.add(stage, sequence);
        }
    }

    /**
     * Checks whether the rest of the stages have in-flight data.
     * This method is called when a query returns no data meaning
     * sequences on previous stages cannot match this window (since there's no new data).
     * However sequences on higher stages can, hence this check to know whether
     * it's possible to advance the window early.
     */
    boolean hasCandidates(int stage) {
        for (int i = stage; i < completionStage; i++) {
            if (stageToKeys.isEmpty(i) == false) {
                return true;
            }
        }
        return false;
    }


    List<Sequence> completed() {
        return limit != null ? limit.view(completed) : completed;
    }

    void dropUntil() {
        keyToSequences.dropUntil();
    }

    void until(Iterable<KeyAndOrdinal> markers) {
        keyToSequences.until(markers);
    }

    void resetInsertPosition() {
        // when dealing with descending calls
        // update the insert point of all sequences
        // for the next batch of hits which will be sorted ascending
        // yet will occur _before_ the current batch
        if (descending) {
            keyToSequences.resetGroupInsertPosition();
            keyToSequences.resetUntilInsertPosition();

            completedInsertPosition = 0;
        }
    }

    public Stats stats() {
        return stats;
    }

    public void clear() {
        stats.clear();
        keyToSequences.clear();
        stageToKeys.clear();
        completed.clear();
    }

    @Override
    public String toString() {
        return LoggerMessageFormat.format(null, "Tracking [{}] keys with [{}] completed and in-flight {}",
                keyToSequences,
                completed.size(),
                stageToKeys);
    }
}