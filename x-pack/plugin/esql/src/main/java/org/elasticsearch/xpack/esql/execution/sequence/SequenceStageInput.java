/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SequenceCondition;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStageProjection;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.CANDIDATE_TRANSLATOR_HANDLER;

/**
 * Runs a resolved stage's ordinary ES|QL row operators over one bounded source page. A hidden row marker survives
 * projections and expansion, linking transformed output back to the reader's original timestamp and PIT ordinal.
 * Visible expression IDs stay unchanged. Only fields needed by the selected payloads, keys and predicates are read.
 * <p>
 * Union inputs are processed one source branch at a time, remapping their output IDs to the union's schema. Branch
 * mappings are prepared lazily and reused across pages. Predicate subquery tables belong to the prepared stage and
 * are not treated as event sources. The owning execution serializes preparation and binding; one binding per branch
 * may be active at a time.
 */
public final class SequenceStageInput {
    private final SequenceStage stage;
    private final String stageLabel;
    private final List<EsRelation> eventSources;
    private final Template[] templates;
    private final List<Attribute> visibleOutput;
    private final List<Attribute> mandatoryPayloads;
    private final List<Attribute> retentionPayloads;
    private final LongConsumer retentionAdmission;
    private final Attribute rowMarker;

    /** Prepares all visible stage payloads, together with its keys and predicates. */
    public SequenceStageInput(SequenceStage stage) {
        this(stage, stage.output());
    }

    /** Prepares only payloads retained by the sequence's wide output; correlation keys are retained separately. */
    public SequenceStageInput(SequenceStage stage, List<Attribute> payloads) {
        this(stage, payloads, 0);
    }

    /** The source operator supplies the query-local position without adding diagnostic state to plan serialization. */
    SequenceStageInput(SequenceStage stage, List<Attribute> payloads, int stageNumber) {
        this(stage, payloads, List.copyOf(payloads), List.of(), unused -> {}, stageNumber);
    }

    /** Demanded output values are evaluated with the event and kept independently of condition values. */
    public static SequenceStageInput selection(SequenceStage stage, List<Attribute> payloads) {
        return selection(stage, payloads, unused -> {});
    }

    /** The query owner reserves bounded proof/description metadata until its terminal cleanup, before lazy allocation. */
    public static SequenceStageInput selection(SequenceStage stage, List<Attribute> payloads, LongConsumer admission) {
        return selection(stage, payloads, admission, 0);
    }

    /** Uses the owning command's 1-based stage position for query-local retention diagnostics. */
    static SequenceStageInput selection(SequenceStage stage, List<Attribute> payloads, LongConsumer admission, int stageNumber) {
        return new SequenceStageInput(stage, payloads, List.copyOf(payloads), List.of(), admission, stageNumber);
    }

    /** Mandatory predicate values survive selection independently of retained output values. */
    static SequenceStageInput selection(
        SequenceStage stage,
        List<Attribute> optionalPayloads,
        List<SequenceLatestValidConfig.ValueDemand> mandatoryDemands,
        LongConsumer admission,
        int stageNumber
    ) {
        List<Attribute> mandatory = canonicalProjection(
            stage,
            mandatoryDemands.stream().map(SequenceLatestValidConfig.ValueDemand::attribute).toList(),
            List.of(),
            "stage [" + stageNumber + "]"
        );
        return new SequenceStageInput(stage, optionalPayloads, List.copyOf(optionalPayloads), mandatory, admission, stageNumber);
    }

    private SequenceStageInput(
        SequenceStage stage,
        List<Attribute> payloads,
        List<Attribute> retentionPayloads,
        List<Attribute> mandatoryPayloads,
        LongConsumer retentionAdmission,
        int stageNumber
    ) {
        if (stage.resolved() == false) {
            throw new IllegalArgumentException("sequence stage input must be resolved");
        }
        if (stageNumber < 0) {
            throw new IllegalArgumentException("sequence stage number must be nonnegative");
        }
        this.stage = stage;
        stageLabel = stageNumber == 0
            ? "stage at [line " + stage.source().source().getLineNumber() + ", column " + stage.source().source().getColumnNumber() + "]"
            : "stage [" + stageNumber + "]";
        this.retentionPayloads = canonicalProjection(stage, retentionPayloads, List.of(), stageLabel);
        this.mandatoryPayloads = mandatoryPayloads;
        this.retentionAdmission = Objects.requireNonNull(retentionAdmission);
        visibleOutput = SequenceStageProjection.selectionOutput(stage, payloads, mandatoryPayloads);
        eventSources = SequenceStageProjection.eventSources(stage);
        templates = new Template[eventSources.size()];
        rowMarker = new ReferenceAttribute(
            Source.EMPTY,
            null,
            TemporaryNameGenerator.locallyUniqueTemporaryName("sequence_source_row"),
            DataType.LONG,
            Nullability.FALSE,
            null,
            true
        );
    }

    public int sourceCount() {
        return eventSources.size();
    }

    /** Returns a source descriptor whose output is the compact raw-page schema, excluding internal identity. */
    public EsRelation source(int branch) {
        return template(branch).source;
    }

    /**
     * Conservative source predicates evaluated before fetching a page. The ordinary stage still rechecks every
     * predicate. Only references to this branch's original fields qualify; computed and expanded attributes carry
     * different expression IDs and cannot accidentally constrain the untransformed source document.
     */
    public QueryBuilder searchQuery(int branch) {
        return template(branch).query;
    }

    /** Original source timestamp resolution, independent of payload pruning and stage projections. */
    public DataType timestampType(int branch) {
        template(branch); // Validate the branch consistently with source() and bind().
        return eventSources.get(branch).output().stream().filter(a -> a.name().equals("@timestamp")).findFirst().orElseThrow().dataType();
    }

    public Attribute rowMarker() {
        return rowMarker;
    }

    /** The final marker and mandatory condition inputs are private; selected column IDs and types stay unchanged. */
    public List<Attribute> output() {
        List<Attribute> attributes = new ArrayList<>(visibleOutput);
        attributes.add(rowMarker);
        return List.copyOf(attributes);
    }

    /** A union branch exports only values proven to belong to its own selection closure. */
    public List<Attribute> output(int branch) {
        return template(branch).plan.output();
    }

    /** Optimizer aliases can change an attribute's Java class while preserving its unique value identity. */
    static int outputChannel(List<Attribute> attributes, Attribute expected, String stage) {
        int channel = -1;
        for (int current = 0; current < attributes.size(); current++) {
            if (expected.id().equals(attributes.get(current).id())) {
                if (channel >= 0) {
                    throw new IllegalStateException(
                        "SEQUENCE " + stage + " output has duplicate column identity [" + expected.name() + "#" + expected.id() + "]"
                    );
                }
                channel = current;
            }
        }
        if (channel < 0) {
            throw new IllegalStateException("SEQUENCE " + stage + " output lost column [" + expected.name() + "#" + expected.id() + "]");
        }
        if (attributes.get(channel).dataType() != expected.dataType()) {
            throw new IllegalStateException(
                "SEQUENCE " + stage + " output changed type of column [" + expected.name() + "#" + expected.id() + "]"
            );
        }
        return channel;
    }

    /** Demanded stage outputs use the canonical optimized identities in every source branch. */
    public List<Attribute> retained(int branch) {
        template(branch);
        return retentionPayloads;
    }

    /**
     * Borrows a raw page whose columns match the compact source descriptor. The binding owns an independent
     * reference to its input blocks; the driver acquires separate references when execution starts.
     * Close the binding after execution, or on planning, execution or cancellation failure.
     */
    public Bound bind(int source, Page raw, BlockFactory blockFactory) {
        Template template = template(source);
        if (raw.getBlockCount() != template.source.output().size()) {
            throw new IllegalArgumentException("sequence source page does not match its resolved input schema");
        }
        long bytes = 64L + SequenceMemoryBudget.pageBytes(raw.getBlockCount() + 1);
        var breaker = blockFactory.breaker();
        breaker.addEstimateBytesAndMaybeBreak(bytes, "esql_sequence_bound_page");
        Page input = null;
        boolean success = false;
        try {
            input = withRowPositions(raw, blockFactory);
            template.slot.bind(input);
            var result = new Bound(template, input, breaker, bytes);
            success = true;
            return result;
        } finally {
            if (success == false) {
                try {
                    Releasables.close(input);
                } finally {
                    breaker.addWithoutBreaking(-bytes);
                }
            }
        }
    }

    private Template template(int branch) {
        if (branch < 0 || branch >= eventSources.size()) {
            throw new IllegalArgumentException("invalid sequence source branch [" + branch + "]");
        }
        Template template = templates[branch];
        if (template == null) {
            if (retentionPayloads.isEmpty() == false || mandatoryPayloads.isEmpty() == false) {
                // Covers the demanded projection and compiled branch description, including failed preparation.
                retentionAdmission.accept(retentionPlanBytes(stage.child()));
            }
            EsRelation selected = eventSources.get(branch);
            var slot = new InputSlot();
            var prepared = prepare(visibleOutput, selected, slot);
            List<Expression> predicates = new ArrayList<>();
            collectSourcePredicates(stage.child(), selected, predicates);
            QueryBuilder query = predicates.isEmpty()
                ? null
                : CANDIDATE_TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, Predicates.combineAnd(predicates))
                    .toQueryBuilder();
            template = new Template(
                prepared.source(),
                new Mapper().map(new Versioned<>(prepared.plan(), TransportVersion.current())),
                slot,
                query
            );
            templates[branch] = template;
        }
        return template;
    }

    private SequenceStageProjection.Prepared prepare(List<Attribute> requested, EsRelation selected, InputSlot slot) {
        List<Attribute> publicPayloads = requested.stream().filter(stage.outputSet()::contains).toList();
        return SequenceStageProjection.prepare(stage, publicPayloads, mandatoryPayloads, selected, rowMarker, slot);
    }

    private static long retentionPlanBytes(LogicalPlan plan) {
        long bytes = Math.addExact(1024L, Math.multiplyExact(384L, plan.output().size()));
        List<LogicalPlan> children = plan instanceof AbstractSubqueryJoin join ? List.of(join.left()) : plan.children();
        for (LogicalPlan child : children) {
            bytes = Math.addExact(bytes, retentionPlanBytes(child));
        }
        return bytes;
    }

    /** Resolve demanded identities before pruning, including private own-row values at their WHERE boundary. */
    private static List<Attribute> canonicalProjection(
        SequenceStage stage,
        List<Attribute> first,
        List<Attribute> second,
        String stageLabel
    ) {
        Set<NameId> wanted = new HashSet<>();
        for (List<Attribute> attributes : List.of(first, second)) {
            for (Attribute attribute : attributes) {
                boolean privateInput = stage.child().collect(SequenceCondition.class).stream().anyMatch(condition -> {
                    return condition.activeInputs().stream().anyMatch(input -> {
                        return input.stage() == condition.ownStage() && input.source().id().equals(attribute.id());
                    });
                });
                if (privateInput == false || stage.output().stream().anyMatch(current -> current.id().equals(attribute.id()))) {
                    outputChannel(stage.output(), attribute, stageLabel);
                }
                wanted.add(attribute.id());
            }
        }
        List<Attribute> requested = new ArrayList<>(first);
        requested.addAll(second);
        try {
            return SequenceStageProjection.selectionOutput(stage, List.of(), requested)
                .stream()
                .filter(attribute -> wanted.remove(attribute.id()))
                .toList();
        } catch (IllegalStateException e) {
            throw new IllegalStateException("SEQUENCE " + stageLabel + " mandatory projection is unavailable: " + e.getMessage(), e);
        }
    }

    private Page withRowPositions(Page raw, BlockFactory blockFactory) {
        try (var positions = blockFactory.newLongBlockBuilder(raw.getPositionCount())) {
            for (int row = 0; row < raw.getPositionCount(); row++) {
                positions.appendLong(row);
            }
            var marker = positions.build();
            Page copy = null;
            boolean success = false;
            try {
                copy = raw.shallowCopy();
                Page input = copy.appendBlock(marker);
                success = true; // Transfer the shallow copy's block references and marker to the new page.
                return input;
            } finally {
                if (success == false) {
                    marker.close();
                    if (copy != null) {
                        copy.releaseBlocks();
                    }
                }
            }
        }
    }

    private static boolean collectSourcePredicates(LogicalPlan plan, EsRelation selected, List<Expression> predicates) {
        if (plan instanceof EsRelation relation) {
            return relation == selected;
        }
        boolean active = false;
        List<LogicalPlan> children = plan instanceof AbstractSubqueryJoin join ? List.of(join.left()) : plan.children();
        for (LogicalPlan child : children) {
            active |= collectSourcePredicates(child, selected, predicates);
        }
        if (active && plan instanceof Filter filter) {
            for (Expression predicate : Predicates.splitAnd(filter.condition())) {
                if (selected.outputSet().containsAll(predicate.references())
                    // Score is computed by this Search request, not a mapped source field. A term query on it would
                    // rewrite to match_none before the local metadata predicate can run.
                    && predicate.references().stream().noneMatch(MetadataAttribute::isScoreAttribute)
                    && TranslationAware.translatable(predicate, LucenePushdownPredicates.DEFAULT)
                        .finish() != TranslationAware.FinishedTranslatable.NO) {
                    predicates.add(predicate);
                }
            }
        }
        return active;
    }

    /** Query-local compiled factories share the branch schema, while each binding owns its input and output target. */
    private static final class Template {
        private final EsRelation source;
        private final PhysicalPlan plan;
        private final InputSlot slot;
        private final QueryBuilder query;
        private final OutputSlot output = new OutputSlot();
        private Supplier<List<Driver>> driverFactory;

        private Template(EsRelation source, PhysicalPlan plan, InputSlot slot, QueryBuilder query) {
            this.source = source;
            this.plan = plan;
            this.slot = slot;
            this.query = query;
        }
    }

    /** Stable compiled output handoff; a binding owns only its current target, never a previous page's collector. */
    private static final class OutputSlot implements Consumer<Page> {
        private Consumer<Page> output;

        synchronized void bind(Consumer<Page> target) {
            if (output != null) {
                throw new IllegalStateException("sequence source branch already has an active output binding");
            }
            output = Objects.requireNonNull(target);
        }

        synchronized void unbind() {
            output = null;
        }

        @Override
        public void accept(Page page) {
            Consumer<Page> target;
            synchronized (this) {
                target = output;
            }
            if (target == null) {
                page.releaseBlocks();
                throw new IllegalStateException("sequence source branch output is not bound");
            }
            // The binding survives until driver completion. Invoke outside this slot's monitor because the
            // collector can acquire the owning source monitor, which also owns binding cleanup.
            target.accept(page);
        }
    }

    /** Coordinator-local input handoff. Each driver consumes its own references, separate from the binding's owner. */
    private static final class InputSlot implements LocalSupplier {
        private Page input;

        synchronized void bind(Page page) {
            if (input != null) {
                throw new IllegalStateException("sequence source branch already has an active binding");
            }
            input = page;
        }

        synchronized void unbind(Page page) {
            assert input == page;
            input = null;
        }

        @Override
        public synchronized Page get() {
            if (input == null) {
                throw new IllegalStateException("sequence source branch is not bound");
            }
            return input.shallowCopy();
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("sequence stage input is coordinator-local");
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("sequence stage input is coordinator-local");
        }
    }

    /** Reusable physical stage plan with independently owned input references for this execution only. */
    public static final class Bound implements Releasable {
        private final Template template;
        private final Page input;
        private final CircuitBreaker breaker;
        private final long bytes;
        private boolean outputBound;
        private boolean closed;

        private Bound(Template template, Page input, CircuitBreaker breaker, long bytes) {
            this.template = template;
            this.input = input;
            this.breaker = breaker;
            this.bytes = bytes;
        }

        public PhysicalPlan plan() {
            return template.plan;
        }

        /**
         * Compiles this branch lazily once, then creates fresh drivers and contexts for the current binding.
         * The output target and borrowed input stay bound until terminal completion, including failed creation.
         */
        public synchronized List<Driver> createDrivers(SequenceSourceOperator.StagePlanner planner, Consumer<Page> output) {
            if (closed || outputBound) {
                throw new IllegalStateException("sequence source binding cannot create drivers again");
            }
            template.output.bind(output);
            outputBound = true;
            if (template.driverFactory == null) {
                template.driverFactory = Objects.requireNonNull(planner.plan(template.plan, template.output));
            }
            return template.driverFactory.get();
        }

        @Override
        public synchronized void close() {
            if (closed == false) {
                closed = true;
                try {
                    if (outputBound) {
                        template.output.unbind();
                    }
                } finally {
                    try {
                        template.slot.unbind(input);
                    } finally {
                        try {
                            input.releaseBlocks();
                        } finally {
                            breaker.addWithoutBreaking(-bytes);
                        }
                    }
                }
            }
        }
    }
}
