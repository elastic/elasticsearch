/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;

/** Admits bounded production work separately from configurations reserved for direct matcher tests. */
final class SequenceLatestValidConfig {
    static final int DEFAULT_MAX_CANDIDATES = 8;
    static final int MAX_CANDIDATE_EXPONENT = 20;
    static final int MAX_CANDIDATES = 1024;
    static final long SEARCH_STEP_LIMIT = 131072;

    private final OptionalInt maxCandidates;
    private final long searchStepLimit;
    private long arrivalStepLimit;
    private boolean rememberChains;
    private final boolean testOnly;
    private final List<List<ValueDemand>> mandatoryDemands;
    private final List<PredicateGroup> groups;

    private SequenceLatestValidConfig(
        OptionalInt maxCandidates,
        long searchStepLimit,
        boolean testOnly,
        List<List<ValueDemand>> demands,
        List<PredicateGroup> groups
    ) {
        maximumCandidates(demands.size());
        if (searchStepLimit < 1 || (maxCandidates.isPresent() && maxCandidates.getAsInt() < 1)) {
            throw new IllegalArgumentException("sequence candidate bound and search step limit must be positive");
        }
        this.maxCandidates = maxCandidates;
        this.searchStepLimit = searchStepLimit;
        arrivalStepLimit = searchStepLimit;
        this.testOnly = testOnly;
        List<List<ValueDemand>> copy = new ArrayList<>(demands.size());
        for (List<ValueDemand> stage : demands) {
            Set<NameId> identities = new HashSet<>();
            for (ValueDemand demand : stage) {
                if (identities.add(demand.attribute().id()) == false) {
                    throw new IllegalArgumentException("duplicate mandatory sequence column identity");
                }
            }
            copy.add(List.copyOf(stage));
        }
        mandatoryDemands = List.copyOf(copy);
        this.groups = List.copyOf(groups);
        for (PredicateGroup group : groups) {
            if (group.ownStage() >= demands.size()) {
                throw new IllegalArgumentException("sequence predicate stage is outside the demand layout");
            }
        }
    }

    static SequenceLatestValidConfig defaults(List<List<ValueDemand>> demands, List<PredicateGroup> groups) {
        validateProductionStageCount(demands.size());
        return bounded(DEFAULT_MAX_CANDIDATES, demands, groups);
    }

    static SequenceLatestValidConfig bounded(int maxCandidates, List<List<ValueDemand>> demands, List<PredicateGroup> groups) {
        validateCandidateBound(maxCandidates, demands.size());
        return new SequenceLatestValidConfig(OptionalInt.of(maxCandidates), SEARCH_STEP_LIMIT, false, demands, groups);
    }

    /** Unbounded state and search are never admitted by an ordinary source entry point. */
    static SequenceLatestValidConfig unboundedForTests(List<List<ValueDemand>> demands, List<PredicateGroup> groups) {
        return new SequenceLatestValidConfig(OptionalInt.empty(), Long.MAX_VALUE, true, demands, groups);
    }

    /** Explicit provenance also excludes a test configuration whose numbers happen to equal production defaults. */
    static SequenceLatestValidConfig boundedForTests(
        int maxCandidates,
        long searchStepLimit,
        List<List<ValueDemand>> demands,
        List<PredicateGroup> groups
    ) {
        return new SequenceLatestValidConfig(OptionalInt.of(maxCandidates), searchStepLimit, true, demands, groups);
    }

    /**
     * Select the experimental admission rule. Its default supports at most four stages, whose final search
     * takes at most 3,208 steps at the default candidate bound.
     */
    SequenceLatestValidConfig withRememberedChains(boolean enabled) {
        rememberChains = enabled;
        return this;
    }

    /** Separate arrival work cap is reserved for direct matcher tests. */
    SequenceLatestValidConfig withArrivalLimitForTests(long limit) {
        if (testOnly == false || limit < 1) {
            throw new IllegalArgumentException("arrival work override requires a positive test-only limit");
        }
        arrivalStepLimit = limit;
        return this;
    }

    boolean rememberChains() {
        return rememberChains;
    }

    long arrivalStepLimit() {
        return arrivalStepLimit;
    }

    static void validateProductionStageCount(int stages) {
        if (stages > Sequence.MAX_EXECUTION_STAGES) {
            throw new IllegalArgumentException("SEQUENCE supports at most [" + Sequence.MAX_EXECUTION_STAGES + "] stages");
        }
    }

    /** The hidden setting retains its previous ceiling for tests and measurements above the default. */
    static int maximumCandidates(int stages) {
        if (stages < 2) {
            throw new IllegalArgumentException("sequence requires at least two stages");
        }
        return 1 << Math.min(Integer.numberOfTrailingZeros(MAX_CANDIDATES), MAX_CANDIDATE_EXPONENT / (stages - 1));
    }

    static void validateCandidateBound(int candidates, int stages) {
        int maximum = maximumCandidates(stages);
        if (candidates < 1 || candidates > maximum) {
            throw new IllegalArgumentException(
                "SEQUENCE sequence_max_candidates pragma ["
                    + candidates
                    + "] for ["
                    + stages
                    + "] stages must be between [1] and ["
                    + maximum
                    + "]"
            );
        }
    }

    /** Ordinary exceptions enforce this boundary even when assertions and snapshot checks are disabled. */
    void validateForProduction(int stages) {
        validateProductionStageCount(stages);
        if (testOnly || maxCandidates.isEmpty() || searchStepLimit != SEARCH_STEP_LIMIT || arrivalStepLimit != SEARCH_STEP_LIMIT) {
            throw new IllegalArgumentException("test-only SEQUENCE limits cannot be used by a source operator");
        }
        if (mandatoryDemands.size() != stages) {
            throw new IllegalArgumentException("sequence mandatory demand layouts must align with stages");
        }
        validateCandidateBound(maxCandidates.getAsInt(), stages);
    }

    /** Profile predicate work without changing limits, demand layout, or test-only provenance. */
    SequenceLatestValidConfig withProfiledPredicates(SequenceRuntimeProfiler profiler) {
        List<PredicateGroup> wrapped = new ArrayList<>(groups.size());
        for (PredicateGroup group : groups) {
            wrapped.add(new PredicateGroup(group.ownStage(), group.referencedStages(), values -> {
                long started = profiler.start();
                try {
                    return group.predicate().test(values);
                } finally {
                    profiler.finish(SequenceRuntimeProfiler.Phase.CONDITION, started);
                }
            }));
        }
        SequenceLatestValidConfig profiled = new SequenceLatestValidConfig(
            maxCandidates,
            searchStepLimit,
            testOnly,
            mandatoryDemands,
            wrapped
        );
        profiled.rememberChains = rememberChains;
        profiled.arrivalStepLimit = arrivalStepLimit;
        return profiled;
    }

    long searchStepLimit() {
        return searchStepLimit;
    }

    OptionalInt maxCandidates() {
        return maxCandidates;
    }

    List<List<ValueDemand>> mandatoryDemands() {
        return mandatoryDemands;
    }

    List<PredicateGroup> groups() {
        return groups;
    }

    /** Slot order is caller supplied; identity/type binding belongs to the prepared stage schema. */
    record ValueDemand(Attribute attribute) {
        ValueDemand {
            Objects.requireNonNull(attribute);
        }
    }

    /** A group is callable as soon as its lowest referenced stage has been bound by backward search. */
    record PredicateGroup(int ownStage, List<Integer> referencedStages, RowPredicate predicate) {
        PredicateGroup {
            referencedStages = List.copyOf(referencedStages);
            Objects.requireNonNull(predicate);
            if (ownStage < 1 || referencedStages.isEmpty()) {
                throw new IllegalArgumentException("sequence predicate requires an earlier referenced stage");
            }
            int previous = -1;
            for (int stage : referencedStages) {
                if (stage <= previous || stage >= ownStage) {
                    throw new IllegalArgumentException("sequence predicate references must be sorted unique earlier stages");
                }
                previous = stage;
            }
        }

        int minReferencedStage() {
            return referencedStages.getFirst();
        }
    }

    /** Predicates synchronously borrow their inputs; they must not retain, close, or mutate them. */
    @FunctionalInterface
    interface RowPredicate {
        boolean test(SelectedValues values);
    }

    /** One-position typed blocks; a real null cell is distinct from a missing demanded block. */
    interface SelectedValues {
        Block block(int stage, int demandSlot);
    }
}
