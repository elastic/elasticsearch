/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Per-datafeed mutable runtime state that tracks which linked projects (clusters)
 * participate in a cross-project search (CPS) datafeed across search cycles.
 * <p>
 * Detects project linking (new projects appearing) and unlinking (existing projects
 * disappearing) using a symmetric dual stabilization condition: a project must be
 * consecutively present (or absent) for {@link #STABILIZATION_CYCLES} search cycles
 * <em>and</em> at least {@link #STABILIZATION_FLOOR} wall-clock time must have elapsed
 * since the state change was first observed.
 * <p>
 * Created when the datafeed starts and discarded when it stops. On the first cycle
 * the observed project set is recorded as the baseline with no annotation.
 */
public class CrossProjectSearchStats {

    static final int STABILIZATION_CYCLES = 12;
    static final Duration STABILIZATION_FLOOR = Duration.ofMinutes(5);

    private final Supplier<Instant> clock;

    private int totalProjects;
    private int availableProjects;
    private int skippedProjects;
    private double availabilityRatio;
    private boolean baselineEstablished;

    private final Map<String, Integer> consecutiveSkips = new HashMap<>();
    private final Map<String, StabilizationTracker> linkingCandidates = new HashMap<>();
    private final Map<String, StabilizationTracker> unlinkingCandidates = new HashMap<>();
    private final Set<String> stabilizedProjectAliases = new HashSet<>();

    public CrossProjectSearchStats(Supplier<Instant> clock) {
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Updates tracking state with the results from one search cycle and returns
     * a {@link CycleResult} describing any confirmed scope changes.
     *
     * @param cycle per-project states extracted from the latest {@code SearchResponse};
     *              empty for non-CPS datafeeds (in which case this is a no-op)
     */
    public CycleResult update(List<LinkedProjectState> cycle) {
        if (cycle.isEmpty()) {
            return CycleResult.NO_CHANGE;
        }

        Instant now = clock.get();
        Set<String> currentAliases = new HashSet<>();

        totalProjects = cycle.size();
        int available = 0;
        int skipped = 0;

        for (LinkedProjectState state : cycle) {
            currentAliases.add(state.alias());
            switch (state.status()) {
                case AVAILABLE -> {
                    available++;
                    consecutiveSkips.put(state.alias(), 0);
                }
                case SKIPPED -> {
                    skipped++;
                    consecutiveSkips.merge(state.alias(), 1, Integer::sum);
                }
                case FAILED -> consecutiveSkips.merge(state.alias(), 1, Integer::sum);
            }
        }
        availableProjects = available;
        skippedProjects = skipped;
        availabilityRatio = totalProjects > 0 ? (double) availableProjects / totalProjects : 0.0;

        if (baselineEstablished == false) {
            stabilizedProjectAliases.addAll(currentAliases);
            baselineEstablished = true;
            return CycleResult.NO_CHANGE;
        }

        updateTracking(currentAliases, now, ConfirmationType.LINKING);
        updateTracking(currentAliases, now, ConfirmationType.UNLINKING);

        Instant[] earliest = new Instant[] { null };
        Set<String> newlyStabilized = confirmLinking(now, earliest);
        Set<String> confirmedRemovals = confirmUnlinking(now, earliest);

        cleanupStaleTrackingEntries(currentAliases);

        boolean scopeChanged = newlyStabilized.isEmpty() == false || confirmedRemovals.isEmpty() == false;
        Instant changeTimestamp = scopeChanged ? (earliest[0] != null ? earliest[0] : now) : null;

        return new CycleResult(newlyStabilized, confirmedRemovals, scopeChanged, changeTimestamp);
    }

    private void updateTracking(Set<String> currentAliases, Instant now, ConfirmationType type) {
        Map<String, StabilizationTracker> candidates = type.getCandidates(this);

        Set<String> trackable = type.getTrackableAliases(currentAliases, stabilizedProjectAliases);
        for (String alias : trackable) {
            candidates.compute(alias, (k, v) -> (v == null) ? new StabilizationTracker(now) : v.increment());
        }

        type.cleanupStaleCandidates(candidates, currentAliases);
    }

    private Set<String> confirmLinking(Instant now, Instant[] earliest) {
        return confirmStabilization(now, earliest, ConfirmationType.LINKING);
    }

    private Set<String> confirmUnlinking(Instant now, Instant[] earliest) {
        return confirmStabilization(now, earliest, ConfirmationType.UNLINKING);
    }

    private Set<String> confirmStabilization(Instant now, Instant[] earliest, ConfirmationType type) {
        Set<String> confirmed = new HashSet<>();
        var it = type.getCandidates(this).entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            String alias = entry.getKey();
            StabilizationTracker tracker = entry.getValue();
            if (tracker.isStabilized(now)) {
                confirmed.add(alias);
                type.updateStabilizedSet(stabilizedProjectAliases, alias);
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
     * Removes skip tracking entries for projects no longer present and not
     * being tracked for unlinking (they've fully disappeared from view).
     */
    private void cleanupStaleTrackingEntries(Set<String> currentAliases) {
        consecutiveSkips.keySet().retainAll(currentAliases);
    }

    public int getTotalProjects() {
        return totalProjects;
    }

    public int getAvailableProjects() {
        return availableProjects;
    }

    public int getSkippedProjects() {
        return skippedProjects;
    }

    public double getAvailabilityRatio() {
        return availabilityRatio;
    }

    public Set<String> getStabilizedProjectAliases() {
        return Set.copyOf(stabilizedProjectAliases);
    }

    public Map<String, Integer> getConsecutiveSkips() {
        return Map.copyOf(consecutiveSkips);
    }

    /**
     * The result of a single search cycle update.
     */
    public record CycleResult(
        Set<String> newlyStabilizedProjects,
        Set<String> confirmedRemovals,
        boolean scopeChanged,
        Instant changeTimestamp
    ) {

        static final CycleResult NO_CHANGE = new CycleResult(Set.of(), Set.of(), false, null);
    }

    private record StabilizationTracker(Instant firstEvent, int count) {
        StabilizationTracker(Instant firstEvent) {
            this(firstEvent, 1);
        }

        StabilizationTracker increment() {
            return new StabilizationTracker(firstEvent, count + 1);
        }

        boolean isStabilized(Instant now) {
            return count >= STABILIZATION_CYCLES && Duration.between(firstEvent, now).compareTo(STABILIZATION_FLOOR) >= 0;
        }
    }

    private enum ConfirmationType {
        LINKING {
            @Override
            void updateStabilizedSet(Set<String> stabilized, String alias) {
                stabilized.add(alias);
            }

            @Override
            Map<String, StabilizationTracker> getCandidates(CrossProjectSearchStats stats) {
                return stats.linkingCandidates;
            }

            @Override
            Set<String> getTrackableAliases(Set<String> current, Set<String> stabilized) {
                Set<String> newAliases = new HashSet<>(current);
                newAliases.removeAll(stabilized);
                return newAliases;
            }

            @Override
            void cleanupStaleCandidates(Map<String, StabilizationTracker> candidates, Set<String> currentAliases) {
                candidates.keySet().retainAll(currentAliases);
            }
        },
        UNLINKING {
            @Override
            void updateStabilizedSet(Set<String> stabilized, String alias) {
                stabilized.remove(alias);
            }

            @Override
            Map<String, StabilizationTracker> getCandidates(CrossProjectSearchStats stats) {
                return stats.unlinkingCandidates;
            }

            @Override
            Set<String> getTrackableAliases(Set<String> current, Set<String> stabilized) {
                Set<String> absentStabilized = new HashSet<>(stabilized);
                absentStabilized.removeAll(current);
                return absentStabilized;
            }

            @Override
            void cleanupStaleCandidates(Map<String, StabilizationTracker> candidates, Set<String> currentAliases) {
                candidates.keySet().removeAll(currentAliases);
            }
        };

        abstract void updateStabilizedSet(Set<String> stabilized, String alias);

        abstract Map<String, StabilizationTracker> getCandidates(CrossProjectSearchStats stats);

        abstract Set<String> getTrackableAliases(Set<String> current, Set<String> stabilized);

        abstract void cleanupStaleCandidates(Map<String, StabilizationTracker> candidates, Set<String> currentAliases);
    }
}
