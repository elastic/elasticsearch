/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Per-datafeed mutable runtime state that tracks which linked clusters participate
 * in a cross-cluster search (CCS) datafeed across search cycles.
 * <p>
 * Detects cluster linking (new clusters appearing) and unlinking (existing clusters
 * disappearing) using a symmetric dual stabilization condition: a cluster must be
 * consecutively present (or absent) for {@link #DEFAULT_STABILIZATION_CYCLES} search
 * cycles <em>and</em> at least {@link #DEFAULT_MIN_STABILIZATION_DURATION_JAVA} wall-clock
 * time must have elapsed since the state change was first observed.
 * <p>
 * Created when the datafeed starts and discarded when it stops. On the first search
 * cycle the observed cluster set is recorded as the baseline with no annotation.
 */
public class CrossClusterSearchStats {

    public static final int DEFAULT_STABILIZATION_CYCLES = 12;
    public static final TimeValue DEFAULT_MIN_STABILIZATION_DURATION = TimeValue.timeValueMinutes(5);
    private static final Duration DEFAULT_MIN_STABILIZATION_DURATION_JAVA = Duration.ofMillis(DEFAULT_MIN_STABILIZATION_DURATION.millis());

    private final Supplier<Instant> clock;
    private final int stabilizationCycles;
    private final Duration minStabilizationDuration;

    private int totalClusters;
    private int availableClusters;
    private int skippedClusters;
    private double availabilityRatio;
    private boolean baselineEstablished;

    private final Map<String, Integer> consecutiveUnavailable = new HashMap<>();
    private final Map<String, StabilizationTracker> pendingLinks = new HashMap<>();
    private final Map<String, StabilizationTracker> pendingUnlinks = new HashMap<>();
    private final Set<String> confirmedAliases = new HashSet<>();

    /**
     * Creates stats with production-default stabilization thresholds.
     */
    public CrossClusterSearchStats(Supplier<Instant> clock) {
        this(clock, DEFAULT_STABILIZATION_CYCLES, DEFAULT_MIN_STABILIZATION_DURATION_JAVA);
    }

    /**
     * Creates stats with configurable stabilization thresholds. Intended for integration
     * tests that need faster stabilization without waiting for production timeouts.
     *
     * @param stabilizationCycles       minimum consecutive cycles a change must persist before confirmation
     * @param minStabilizationDuration  minimum wall-clock duration since first observation before confirmation
     */
    public CrossClusterSearchStats(Supplier<Instant> clock, int stabilizationCycles, Duration minStabilizationDuration) {
        this.clock = Objects.requireNonNull(clock);
        if (stabilizationCycles < 1) {
            throw new IllegalArgumentException("stabilizationCycles must be >= 1, got " + stabilizationCycles);
        }
        this.stabilizationCycles = stabilizationCycles;
        this.minStabilizationDuration = Objects.requireNonNull(minStabilizationDuration);
    }

    /**
     * Updates tracking state with the results from one search cycle and returns
     * a {@link ScopeChangeResult} describing any confirmed scope changes.
     *
     * @param linkedClusterStates per-cluster states extracted from the latest {@code SearchResponse};
     *                            empty for local-only datafeeds (in which case this is a no-op)
     */
    public ScopeChangeResult update(List<LinkedClusterState> linkedClusterStates) {
        if (linkedClusterStates.isEmpty()) {
            return ScopeChangeResult.NO_CHANGE;
        }

        Instant now = clock.get();
        Set<String> currentAliases = new HashSet<>();

        // Tally per-cluster availability for this cycle
        totalClusters = linkedClusterStates.size();
        int available = 0;
        int skipped = 0;

        for (LinkedClusterState state : linkedClusterStates) {
            currentAliases.add(state.alias());
            switch (state.status()) {
                case AVAILABLE -> {
                    available++;
                    consecutiveUnavailable.put(state.alias(), 0);
                }
                case SKIPPED -> {
                    skipped++;
                    consecutiveUnavailable.merge(state.alias(), 1, Integer::sum);
                }
                case FAILED -> consecutiveUnavailable.merge(state.alias(), 1, Integer::sum);
            }
        }
        availableClusters = available;
        skippedClusters = skipped;
        availabilityRatio = totalClusters > 0 ? (double) availableClusters / totalClusters : 0.0;

        // First cycle: record the baseline set with no scope change
        if (baselineEstablished == false) {
            confirmedAliases.addAll(currentAliases);
            baselineEstablished = true;
            return ScopeChangeResult.NO_CHANGE;
        }

        // Advance pending link/unlink counters for aliases that are new or missing
        updateTracking(currentAliases, now, ConfirmationType.LINKING);
        updateTracking(currentAliases, now, ConfirmationType.UNLINKING);

        // Promote any candidates that have met both the cycle and duration thresholds
        Instant[] earliest = new Instant[] { null };
        Set<String> confirmedLinks = confirmStabilization(now, earliest, ConfirmationType.LINKING);
        Set<String> confirmedUnlinks = confirmStabilization(now, earliest, ConfirmationType.UNLINKING);

        // Prune skip counters for clusters no longer present
        removeStaleTrackingEntries(currentAliases);

        boolean scopeChanged = confirmedLinks.isEmpty() == false || confirmedUnlinks.isEmpty() == false;
        Instant changeTimestamp = scopeChanged ? (earliest[0] != null ? earliest[0] : now) : null;

        return new ScopeChangeResult(confirmedLinks, confirmedUnlinks, scopeChanged, changeTimestamp);
    }

    /**
     * Advances stabilization counters for one confirmation direction (linking or unlinking).
     * <p>
     * For each alias that is a candidate (new for linking, missing for unlinking), either
     * creates a new tracker or increments the existing counter. Stale candidates whose
     * precondition is no longer met (e.g. a linking candidate that disappeared) are removed.
     */
    private void updateTracking(Set<String> currentAliases, Instant now, ConfirmationType type) {
        Map<String, StabilizationTracker> candidates = type.getCandidates(this);

        Set<String> pending = type.getPendingAliases(currentAliases, confirmedAliases);
        for (String alias : pending) {
            candidates.compute(alias, (k, v) -> (v == null) ? new StabilizationTracker(now) : v.increment());
        }

        type.cleanupStaleCandidates(candidates, currentAliases);
    }

    /**
     * Checks all candidates of the given type and promotes those that have met both the
     * required cycle count and the minimum wall-clock duration. Promoted aliases are moved
     * from the candidate map into (or out of) {@link #confirmedAliases}.
     *
     * @param earliest single-element array to track the earliest first-observation timestamp
     *                 across all promoted candidates in this cycle
     */
    private Set<String> confirmStabilization(Instant now, Instant[] earliest, ConfirmationType type) {
        Set<String> confirmed = new HashSet<>();
        var it = type.getCandidates(this).entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            String alias = entry.getKey();
            StabilizationTracker tracker = entry.getValue();
            if (tracker.isStabilized(now, stabilizationCycles, minStabilizationDuration)) {
                confirmed.add(alias);
                type.updateConfirmedSet(confirmedAliases, alias);
                updateEarliest(earliest, tracker.firstEvent());
                it.remove();
            }
        }
        return confirmed;
    }

    private static void updateEarliest(Instant[] earliest, Instant candidate) {
        if (earliest[0] == null || candidate.isBefore(earliest[0])) {
            earliest[0] = candidate;
        }
    }

    /**
     * Removes unavailability tracking entries for clusters no longer present in
     * the current cycle's aliases.
     */
    private void removeStaleTrackingEntries(Set<String> currentAliases) {
        consecutiveUnavailable.keySet().retainAll(currentAliases);
    }

    public int getTotalClusters() {
        return totalClusters;
    }

    public int getAvailableClusters() {
        return availableClusters;
    }

    public int getSkippedClusters() {
        return skippedClusters;
    }

    public double getAvailabilityRatio() {
        return availabilityRatio;
    }

    public Set<String> getConfirmedAliases() {
        return Set.copyOf(confirmedAliases);
    }

    public Map<String, Integer> getConsecutiveUnavailable() {
        return Map.copyOf(consecutiveUnavailable);
    }

    /**
     * Builds the annotation message for a confirmed scope change, selecting the appropriate
     * template based on whether clusters were linked, unlinked, or both.
     *
     * @param scopeChange a {@link ScopeChangeResult} where {@code scopeChanged} is {@code true}
     * @return the formatted annotation message
     */
    public static String buildScopeChangeMessage(ScopeChangeResult scopeChange) {
        String linked = String.join(", ", new TreeSet<>(scopeChange.confirmedLinks()));
        String unlinked = String.join(", ", new TreeSet<>(scopeChange.confirmedUnlinks()));
        if (linked.isEmpty() == false && unlinked.isEmpty() == false) {
            return Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_SCOPE_CHANGED_BOTH, linked, unlinked);
        } else if (linked.isEmpty() == false) {
            return Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_SCOPE_CHANGED_LINKED, linked);
        } else {
            return Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_SCOPE_CHANGED_UNLINKED, unlinked);
        }
    }

    /**
     * The result of processing a single search cycle through {@link #update}. Contains the sets
     * of newly confirmed linked and unlinked cluster aliases, along with the earliest timestamp
     * at which the change was first observed.
     */
    public record ScopeChangeResult(
        Set<String> confirmedLinks,
        Set<String> confirmedUnlinks,
        boolean scopeChanged,
        Instant changeTimestamp
    ) {

        static final ScopeChangeResult NO_CHANGE = new ScopeChangeResult(Set.of(), Set.of(), false, null);
    }

    private record StabilizationTracker(Instant firstEvent, int count) {
        StabilizationTracker(Instant firstEvent) {
            this(firstEvent, 1);
        }

        StabilizationTracker increment() {
            return new StabilizationTracker(firstEvent, count + 1);
        }

        boolean isStabilized(Instant now, int requiredCycles, Duration requiredDuration) {
            return count >= requiredCycles && Duration.between(firstEvent, now).compareTo(requiredDuration) >= 0;
        }
    }

    private enum ConfirmationType {
        LINKING {
            @Override
            void updateConfirmedSet(Set<String> confirmed, String alias) {
                confirmed.add(alias);
            }

            @Override
            Map<String, StabilizationTracker> getCandidates(CrossClusterSearchStats stats) {
                return stats.pendingLinks;
            }

            @Override
            Set<String> getPendingAliases(Set<String> current, Set<String> confirmed) {
                Set<String> newAliases = new HashSet<>(current);
                newAliases.removeAll(confirmed);
                return newAliases;
            }

            @Override
            void cleanupStaleCandidates(Map<String, StabilizationTracker> candidates, Set<String> currentAliases) {
                candidates.keySet().retainAll(currentAliases);
            }
        },
        UNLINKING {
            @Override
            void updateConfirmedSet(Set<String> confirmed, String alias) {
                confirmed.remove(alias);
            }

            @Override
            Map<String, StabilizationTracker> getCandidates(CrossClusterSearchStats stats) {
                return stats.pendingUnlinks;
            }

            @Override
            Set<String> getPendingAliases(Set<String> current, Set<String> confirmed) {
                Set<String> absentConfirmed = new HashSet<>(confirmed);
                absentConfirmed.removeAll(current);
                return absentConfirmed;
            }

            @Override
            void cleanupStaleCandidates(Map<String, StabilizationTracker> candidates, Set<String> currentAliases) {
                candidates.keySet().removeAll(currentAliases);
            }
        };

        abstract void updateConfirmedSet(Set<String> confirmed, String alias);

        abstract Map<String, StabilizationTracker> getCandidates(CrossClusterSearchStats stats);

        abstract Set<String> getPendingAliases(Set<String> current, Set<String> confirmed);

        abstract void cleanupStaleCandidates(Map<String, StabilizationTracker> candidates, Set<String> currentAliases);
    }
}
