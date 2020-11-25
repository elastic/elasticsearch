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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Matcher of sequences. Keeps track of on-going sequences and advancing them through each stage.
 */
public class SequenceMatcher {

    private final Logger log = LogManager.getLogger(SequenceMatcher.class);

    static class Stats {
        long seen = 0;
        long ignored = 0;
        long rejectionMaxspan = 0;
        long rejectionUntil = 0;

        @Override
        public String toString() {
            return LoggerMessageFormat.format(null, "Stats: Seen [{}]/Ignored [{}]/Rejected {Maxspan [{}]/Until [{}]}",
                    seen,
                    ignored,
                    rejectionMaxspan,
                    rejectionUntil);
        }

        public void clear() {
            seen = 0;
            ignored = 0;
            rejectionMaxspan = 0;
            rejectionUntil = 0;
        }
    }

    // Current sequences for each key
    // Note will be multiple sequences for the same key and the same stage with different timestamps
    private final KeyToSequences keyToSequences;
    // Current keys on each stage
    private final StageToKeys stageToKeys;

    private final int numberOfStages;
    private final int completionStage;

    // Set of completed sequences - separate to avoid polluting the other stages
    // It is a set since matches are ordered at insertion time based on the ordinal of the first entry
    private final Set<Sequence> completed;
    private final long maxSpanInMillis;

    private final boolean descending;

    private final Limit limit;
    private boolean headLimit = false;

    private final Stats stats = new Stats();

    @SuppressWarnings("rawtypes")
    public SequenceMatcher(int stages, boolean descending, TimeValue maxSpan, Limit limit) {
        this.numberOfStages = stages;
        this.completionStage = stages - 1;

        this.descending = descending;
        this.stageToKeys = new StageToKeys(completionStage);
        this.keyToSequences = new KeyToSequences(completionStage);
        this.completed = new TreeSet<>();

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
            keyToSequences.remove(previousStage, key);
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
            Ordinal nearestUntil = until.before(ordinal);
            if (nearestUntil != null) {
                // check if until matches
                if (nearestUntil.between(sequence.ordinal(), ordinal)) {
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

            completed.add(sequence);
            // update the bool lazily
            // only consider positive limits / negative ones imply tail which means having to go
            // through the whole page of results before selecting the last ones
            // doing a limit early returns the 'head' not 'tail'
            headLimit = limit != null && limit.limit() > 0 && completed.size() == limit.totalLimit();
        } else {
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
    boolean hasFollowingCandidates(int stage) {
        return hasCandidates(stage, completionStage);
    }

    /**
     * Checks whether the previous stages still have in-flight data.
     * Used to see whether, after rebasing a window it makes sense to continue finding matches.
     * If there are no in-progress windows, any future results are unnecessary.
     */
    boolean hasCandidates() {
        return hasCandidates(0, completionStage);
    }

    private boolean hasCandidates(int start, int stop) {
        for (int i = start; i < stop; i++) {
            if (stageToKeys.isEmpty(i) == false) {
                return true;
            }
        }
        return false;
    }

    List<Sequence> completed() {
        List<Sequence> asList = new ArrayList<>(completed);
        return limit != null ? limit.view(asList) : asList;
    }

    void until(Iterable<KeyAndOrdinal> markers) {
        keyToSequences.until(markers);
    }

    /**
     * Called when moving to a new page.
     * This allows the matcher to keep only the last match per stage
     * and adjust insertion positions.
     */
    void trim(boolean everything) {
        // for descending sequences, remove all in-flight sequences
        // since the windows moves head and thus there is no chance
        // of new results coming in

        // however this needs to be indicated from outside since
        // the same window can be only ASC trimmed during a loop
        // and fully once the DESC query moves
        if (everything) {
            keyToSequences.clear();
        } else {
            // keep only the tail
            keyToSequences.trimToTail();
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
        return LoggerMessageFormat.format(null, "Tracking [{}] keys with [{}] completed and {} in-flight",
                keyToSequences,
                completed.size(),
                stageToKeys);
    }
}
