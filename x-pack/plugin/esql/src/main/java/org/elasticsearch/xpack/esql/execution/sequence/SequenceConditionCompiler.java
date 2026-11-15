/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceCondition;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Compiles deferred conditions once per owning source. Original conjunct boundaries fix when each group runs;
 * optimized scalar references alone determine which typed values candidates must keep. Calls and closure are
 * serialized by that source, just like the matcher that borrows the resulting predicates.
 */
final class SequenceConditionCompiler implements Releasable {
    private static final String LABEL = "esql_sequence_conditions";
    private static final long PAGE_BYTES = RamUsageEstimator.shallowSizeOfInstance(Page.class);

    private final DriverContext ownerContext;
    private final CircuitBreaker breaker;
    private final List<CompiledGroup> compiled = new ArrayList<>();
    private DriverContext evaluationContext;
    private SequenceLatestValidConfig config;
    private long heldBytes;
    private boolean closed;

    private SequenceConditionCompiler(DriverContext ownerContext, @Nullable CircuitBreaker scopedBreaker) {
        this.ownerContext = ownerContext;
        breaker = scopedBreaker == null ? LocalCircuitBreaker.forAsyncIo(ownerContext.breaker()) : scopedBreaker;
    }

    /** Compiles native predicates and typed demands into one closeable owner for a source's matcher. */
    static SequenceConditionCompiler compile(
        Sequence sequence,
        int maxCandidates,
        FoldContext foldContext,
        DriverContext ownerContext,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        return compile(sequence, maxCandidates, foldContext, ownerContext, analysisRegistry, null);
    }

    /** Uses the source's explicit-admission scope without replacing the predicate evaluator's factory or allocator. */
    static SequenceConditionCompiler compile(
        Sequence sequence,
        int maxCandidates,
        FoldContext foldContext,
        DriverContext ownerContext,
        @Nullable AnalysisRegistry analysisRegistry,
        @Nullable CircuitBreaker scopedBreaker
    ) {
        var result = new SequenceConditionCompiler(ownerContext, scopedBreaker);
        try {
            result.prepare(sequence, maxCandidates, foldContext, analysisRegistry);
            return result;
        } catch (RuntimeException | Error e) {
            try {
                result.close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
    }

    private void prepare(Sequence sequence, int maxCandidates, FoldContext foldContext, @Nullable AnalysisRegistry analysisRegistry) {
        // Includes the owner, warning context/budget, per-stage maps and overlapping immutable demand lists.
        reserve(2048L + 256L * sequence.children().size());
        evaluationContext = new DriverContext(
            ownerContext.bigArrays(),
            ownerContext.blockFactory().parent(),
            ownerContext.localBreakerSettings(),
            "sequence conditions"
        ) {
            @Override
            public WarningsMode warningsMode() {
                return ownerContext.warningsMode();
            }
        };
        evaluationContext.setWarningsBudget(new Warnings.Budget());
        evaluationContext.initializeEarlyTerminationChecker(ownerContext::checkForEarlyTermination);
        List<Map<NameId, Integer>> slots = new ArrayList<>();
        List<List<SequenceLatestValidConfig.ValueDemand>> demands = new ArrayList<>();
        for (int stage = 0; stage < sequence.children().size(); stage++) {
            slots.add(new LinkedHashMap<>());
            demands.add(new ArrayList<>());
        }
        Map<GroupKey, PendingGroup> pending = new LinkedHashMap<>();
        for (int stage = 0; stage < sequence.children().size(); stage++) {
            SequenceStage source = (SequenceStage) sequence.children().get(stage);
            for (SequenceCondition condition : source.child().collect(SequenceCondition.class)) {
                ownerContext.checkForEarlyTermination();
                if (condition.ownStage() != stage
                    || condition.resolved() == false
                    || (condition.condition().dataType() != DataType.BOOLEAN && condition.condition().dataType() != DataType.NULL)) {
                    throw new IllegalArgumentException("SEQUENCE condition must be resolved against its owning stage as a boolean");
                }
                // Protect temporary reference sets, grouped expressions/layouts and the final runtime mapping.
                reserve(512L + 512L * condition.inputs().size());
                GroupKey key = new GroupKey(stage, condition.referencedStages().getFirst());
                PendingGroup group = pending.computeIfAbsent(key, unused -> new PendingGroup());
                group.conditions.add(condition.condition());
                group.referencedStages.addAll(condition.referencedStages());
                Set<NameId> unresolved = new java.util.HashSet<>();
                condition.condition().references().forEach(attribute -> unresolved.add(attribute.id()));
                for (SequenceCondition.Input input : condition.activeInputs()) {
                    Attribute canonical = canonical(sequence, condition, input);
                    Map<NameId, Integer> stageSlots = slots.get(input.stage());
                    Integer slot = stageSlots.get(canonical.id());
                    if (slot == null) {
                        slot = demands.get(input.stage()).size();
                        stageSlots.put(canonical.id(), slot);
                        demands.get(input.stage()).add(new SequenceLatestValidConfig.ValueDemand(canonical));
                    } else if (demands.get(input.stage()).get(slot).attribute().dataType() != canonical.dataType()) {
                        throw new IllegalStateException("SEQUENCE condition demand changed type");
                    }
                    Channel previous = group.channels.putIfAbsent(
                        input.reference().id(),
                        new Channel(input.reference(), input.stage(), slot, PlannerUtils.toElementType(canonical.dataType()))
                    );
                    if (previous != null) {
                        throw new IllegalStateException("SEQUENCE condition references must have unique identities");
                    }
                    unresolved.remove(input.reference().id());
                }
                if (unresolved.isEmpty() == false) {
                    throw new IllegalStateException("SEQUENCE condition has an input with no stage binding");
                }
            }
        }
        List<SequenceLatestValidConfig.PredicateGroup> groups = new ArrayList<>();
        for (var entry : pending.entrySet()) {
            ownerContext.checkForEarlyTermination();
            PendingGroup pendingGroup = entry.getValue();
            Layout.Builder layout = new Layout.Builder();
            pendingGroup.channels.values().forEach(channel -> layout.append(channel.reference));
            Expression expression = Predicates.combineAnd(pendingGroup.conditions);
            ExpressionEvaluator evaluator = EvalMapper.toEvaluator(foldContext, expression, layout.build(), analysisRegistry)
                .get(evaluationContext);
            boolean accepted = false;
            try {
                reserve(evaluator.baseRamBytesUsed());
                CompiledGroup group = new CompiledGroup(evaluator, pendingGroup.channels.values().toArray(Channel[]::new));
                compiled.add(group);
                accepted = true;
                groups.add(
                    new SequenceLatestValidConfig.PredicateGroup(
                        entry.getKey().ownStage,
                        List.copyOf(pendingGroup.referencedStages),
                        group::test
                    )
                );
            } catch (RuntimeException | Error e) {
                if (accepted == false) {
                    try {
                        evaluator.close();
                    } catch (RuntimeException | Error cleanup) {
                        if (cleanup != e) {
                            e.addSuppressed(cleanup);
                        }
                    }
                }
                throw e;
            }
        }
        config = SequenceLatestValidConfig.bounded(maxCandidates, demands, groups);
    }

    private static Attribute canonical(Sequence sequence, SequenceCondition condition, SequenceCondition.Input input) {
        List<Attribute> output = input.stage() == condition.ownStage()
            ? condition.child().output()
            : sequence.children().get(input.stage()).output();
        String stage = "stage [" + (input.stage() + 1) + "] condition";
        return output.get(SequenceStageInput.outputChannel(output, input.source(), stage));
    }

    SequenceLatestValidConfig config() {
        if (closed) {
            throw new IllegalStateException("SEQUENCE condition compiler is closed");
        }
        return config;
    }

    private void reserve(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, LABEL);
        heldBytes += bytes;
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            try {
                Releasables.close(compiled);
            } finally {
                try {
                    if (evaluationContext != null) {
                        evaluationContext.finish();
                        evaluationContext.getSnapshot().close();
                        if (ownerContext.isFinished() == false) {
                            evaluationContext.warnings().forEach(ownerContext::addWarning);
                        }
                    }
                } finally {
                    config = null;
                    compiled.clear();
                    breaker.addWithoutBreaking(-heldBytes, LABEL);
                    heldBytes = 0;
                }
            }
        }
    }

    private record GroupKey(int ownStage, int minReferencedStage) {}

    private static final class PendingGroup {
        private final List<Expression> conditions = new ArrayList<>();
        private final TreeSet<Integer> referencedStages = new TreeSet<>();
        private final Map<NameId, Channel> channels = new LinkedHashMap<>();
    }

    private record Channel(Attribute reference, int stage, int slot, ElementType type) {}

    /** A call owns temporary page references; neither the evaluator nor its returned block owns the candidate. */
    private final class CompiledGroup implements Releasable {
        private final ExpressionEvaluator evaluator;
        private final Channel[] channels;
        private final Block[] inputs;
        private final long pageBytes;

        private CompiledGroup(ExpressionEvaluator evaluator, Channel[] channels) {
            this.evaluator = evaluator;
            this.channels = channels;
            inputs = new Block[channels.length];
            pageBytes = PAGE_BYTES + RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) channels.length * RamUsageEstimator.NUM_BYTES_OBJECT_REF
            );
        }

        private boolean test(SequenceLatestValidConfig.SelectedValues values) {
            if (closed) {
                throw new IllegalStateException("SEQUENCE condition compiler is closed");
            }
            ownerContext.checkForEarlyTermination();
            breaker.addEstimateBytesAndMaybeBreak(pageBytes, LABEL);
            int acquired = 0;
            try {
                for (Channel channel : channels) {
                    Block block = values.block(channel.stage, channel.slot);
                    if (block == null
                        || block.getPositionCount() != 1
                        || (block.elementType() != channel.type && block.elementType() != ElementType.NULL)) {
                        throw new IllegalStateException("SEQUENCE condition is missing its demanded typed value");
                    }
                    block.incRef();
                    inputs[acquired++] = block;
                }
                try (Page page = new Page(1, inputs)) {
                    acquired = 0; // The page now owns every increment, including aliased channels.
                    try (BooleanBlock result = (BooleanBlock) evaluator.eval(page)) {
                        if (result.getPositionCount() != 1) {
                            throw new IllegalStateException("SEQUENCE condition must produce exactly one position");
                        }
                        return result.isNull(0) == false && result.getValueCount(0) == 1 && result.getBoolean(result.getFirstValueIndex(0));
                    }
                }
            } finally {
                try {
                    if (acquired != 0) {
                        Releasables.close(inputs);
                    }
                } finally {
                    Arrays.fill(inputs, null);
                    breaker.addWithoutBreaking(-pageBytes, LABEL);
                }
            }
        }

        @Override
        public void close() {
            evaluator.close();
        }
    }
}
