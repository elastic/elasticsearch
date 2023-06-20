/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.Timestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Matcher of sequences. Keeps track of on-going sequences and advancing them through each stage.
 */
public class SequenceMatcher {

    private static final String CB_INFLIGHT_LABEL = "sequence_inflight";
    private static final String CB_COMPLETED_LABEL = "sequence_completed";

    private final Logger log = LogManager.getLogger(SequenceMatcher.class);

    static class Stats {

        long seen = 0;
        long ignored = 0;
        long rejectionMaxspan = 0;
        long rejectionUntil = 0;

        @Override
        public String toString() {
            return LoggerMessageFormat.format(
                null,
                "Stats: Seen [{}]/Ignored [{}]/Rejected {Maxspan [{}]/Until [{}]}",
                seen,
                ignored,
                rejectionMaxspan,
                rejectionUntil
            );
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
    private final Set<Sequence> toCheckForMissing;
    private final long maxSpanInNanos;

    private final boolean descending;

    private final Limit limit;
    private final boolean[] missingEventStages;
    protected final int firstPositiveStage;
    protected final int lastPositiveStage;
    private final boolean missingEventStagesExist;
    private final CircuitBreaker circuitBreaker;

    private final Stats stats = new Stats();

    private boolean headLimit = false;

    // circuit breaker accounting
    private long prevRamBytesUsedInFlight = 0;
    private long prevRamBytesUsedCompleted = 0;

    @SuppressWarnings("rawtypes")
    public SequenceMatcher(
        int stages,
        boolean descending,
        TimeValue maxSpan,
        Limit limit,
        boolean[] missingEventStages,
        CircuitBreaker circuitBreaker
    ) {
        this.numberOfStages = stages;
        this.completionStage = stages - 1;

        this.descending = descending;
        this.stageToKeys = new StageToKeys(completionStage);
        this.keyToSequences = new KeyToSequences(completionStage);
        this.completed = new TreeSet<>();
        this.toCheckForMissing = new TreeSet<>();

        this.maxSpanInNanos = maxSpan.nanos();

        this.limit = limit;
        this.missingEventStages = missingEventStages;
        this.firstPositiveStage = calculateFirstPositiveStage();
        this.lastPositiveStage = calculateLastPositiveStage();
        this.missingEventStagesExist = calculateMissingEventStagesExist();
        this.circuitBreaker = circuitBreaker;
    }

    private int calculateFirstPositiveStage() {
        for (int i = 0; i < missingEventStages.length; i++) {
            if (missingEventStages[i] == false) {
                return i;
            }
        }
        return -1;
    }

    private int calculateLastPositiveStage() {
        for (int i = missingEventStages.length - 1; i >= 0; i--) {
            if (missingEventStages[i] == false) {
                return i;
            }
        }
        return -1;
    }

    private boolean calculateMissingEventStagesExist() {
        for (int i = 0; i < missingEventStages.length; i++) {
            if (missingEventStages[i]) {
                return true;
            }
        }
        return false;
    }

    private void trackSequence(Sequence sequence) {
        SequenceKey key = sequence.key();

        stageToKeys.add(firstPositiveStage, key);
        keyToSequences.add(firstPositiveStage, sequence);

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

            if (isFirstPositiveStage(stage)) {
                log.trace("Matching hit {}  - track sequence", ko.ordinal);
                Sequence seq = new Sequence(ko.key, numberOfStages, ko.ordinal, hit);
                if (lastPositiveStage == stage) {
                    tryComplete(seq);
                } else {
                    trackSequence(seq);
                }
            } else {
                log.trace("Matching hit {}  - match", ko.ordinal);
                match(stage, ko.key, ko.ordinal, hit);

                // early skip in case of reaching the limit
                // check the last stage to avoid calling the state machine in other stages
                if (headLimit) {
                    log.trace("(Head) Limit reached {}", stats);
                    return false;
                }
            }
        }

        boolean matched;
        // check tail limit
        if (tailLimitReached()) {
            log.trace("(Tail) Limit reached {}", stats);
            matched = false;
        } else {
            log.trace("{}", stats);
            matched = true;
        }
        trackMemory();
        return matched;
    }

    protected boolean exceedsMaxSpan(Timestamp from, Timestamp to) {
        return maxSpanInNanos > 0 && to.delta(from) > maxSpanInNanos;
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

        int previousStage = previousPositiveStage(stage);
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
        if (exceedsMaxSpan(sequence.startOrdinal().timestamp(), ordinal.timestamp())) {
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
        if (stage == lastPositiveStage) {
            // when dealing with descending queries
            // avoid duplicate matching (since the ASC query can return previously seen results)
            if (descending) {
                for (Sequence seen : completed) {
                    if (seen.key().equals(key) && seen.ordinal().equals(ordinal)) {
                        return;
                    }
                }
            }

            tryComplete(sequence);
            // update the bool lazily
            // only consider positive limits / negative ones imply tail which means having to go
            // through the whole page of results before selecting the last ones
            // doing a limit early returns the 'head' not 'tail'
            calculateHeadLimit();
        } else {
            stageToKeys.add(stage, key);
            keyToSequences.add(stage, sequence);
        }
    }

    public void tryComplete(Sequence sequence) {
        if (missingEventStagesExist) {
            toCheckForMissing.add(sequence);
        } else {
            completed.add(sequence);
        }
    }

    private void calculateHeadLimit() {
        headLimit = limit != null && limit.limit() > 0 && completed.size() == limit.totalLimit();
    }

    int previousPositiveStage(int stage) {
        for (int i = stage - 1; i >= 0; i--) {
            if (missingEventStages[i] == false) {
                return i;
            }
        }
        return -1;
    }

    public boolean limitReached() {
        calculateHeadLimit();
        return headLimit;
    }

    int nextPositiveStage(int stage) {
        for (int i = stage + 1; i < missingEventStages.length; i++) {
            if (missingEventStages[i] == false) {
                return i;
            }
        }
        return -1;
    }

    public boolean isMissingEvent(int stage) {
        if (stage < 0 || stage >= missingEventStages.length) {
            return false;
        }
        return missingEventStages[stage];
    }

    private boolean isFirstPositiveStage(int stage) {
        return stage == firstPositiveStage;
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

    Set<SequenceKey> keys(int stage) {
        return stageToKeys.keys(stage);
    }

    Set<SequenceKey> keys() {
        return stageToKeys.keys();
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
    void trim(Ordinal ordinal) {
        // for descending sequences, remove all in-flight sequences
        // since the windows moves head and thus there is no chance
        // of new results coming in
        if (ordinal == null) {
            keyToSequences.clear();
        } else {
            // keep only the tail
            keyToSequences.trimToTail(ordinal);
        }
    }

    public void addToCompleted(Sequence sequence) {
        this.completed.add(sequence);
    }

    Set<Sequence> toCheckForMissing() {
        return toCheckForMissing;
    }

    public void clear() {
        stats.clear();
        keyToSequences.clear();
        stageToKeys.clear();
        completed.clear();
        toCheckForMissing.clear();
        clearCircuitBreaker();
    }

    // protected for testing purposes
    protected long ramBytesUsedInFlight() {
        return RamUsageEstimator.sizeOf(keyToSequences) + RamUsageEstimator.sizeOf(stageToKeys);
    }

    // protected for testing purposes
    protected long ramBytesUsedCompleted() {
        return RamUsageEstimator.sizeOfCollection(completed);
    }

    private void clearCircuitBreaker() {
        circuitBreaker.addWithoutBreaking(-prevRamBytesUsedInFlight - prevRamBytesUsedCompleted);
        prevRamBytesUsedInFlight = 0;
        prevRamBytesUsedCompleted = 0;
    }

    // The method is called at the end of match() which is called for every sub query in the sequence query
    // and for each subquery every "fetch_size" docs. Doing RAM accounting on object creation is
    // expensive, so we just calculate the difference in bytes of the total memory that the matcher's
    // structure occupy for the in-flight tracking of sequences, as well as for the list of completed
    // sequences.
    private void trackMemory() {
        long newRamBytesUsedInFlight = ramBytesUsedInFlight();
        circuitBreaker.addEstimateBytesAndMaybeBreak(newRamBytesUsedInFlight - prevRamBytesUsedInFlight, CB_INFLIGHT_LABEL);
        prevRamBytesUsedInFlight = newRamBytesUsedInFlight;

        long newRamBytesUsedCompleted = ramBytesUsedCompleted();
        circuitBreaker.addEstimateBytesAndMaybeBreak(newRamBytesUsedCompleted - prevRamBytesUsedCompleted, CB_COMPLETED_LABEL);
        prevRamBytesUsedCompleted = newRamBytesUsedCompleted;
    }

    @Override
    public String toString() {
        return LoggerMessageFormat.format(
            null,
            "Tracking [{}] keys with [{}] completed and {} in-flight",
            keyToSequences,
            completed.size(),
            stageToKeys
        );
    }
}
