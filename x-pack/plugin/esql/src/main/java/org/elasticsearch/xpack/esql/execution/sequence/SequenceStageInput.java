/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
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
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStageProjection;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.ArrayList;
import java.util.List;

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
    private final List<EsRelation> eventSources;
    private final Template[] templates;
    private final List<Attribute> visibleOutput;
    private final Attribute rowMarker;

    /** Prepares all visible stage payloads, together with its keys and predicates. */
    public SequenceStageInput(SequenceStage stage) {
        this(stage, stage.output());
    }

    /** Prepares only payloads retained by the sequence's wide output; correlation keys are retained separately. */
    public SequenceStageInput(SequenceStage stage, List<Attribute> payloads) {
        if (stage.resolved() == false) {
            throw new IllegalArgumentException("sequence stage input must be resolved");
        }
        this.stage = stage;
        visibleOutput = SequenceStageProjection.visibleOutput(stage, payloads);
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

    /** The final marker column is internal; the preceding columns preserve selected stage output IDs and types. */
    public List<Attribute> output() {
        List<Attribute> attributes = new ArrayList<>(visibleOutput);
        attributes.add(rowMarker);
        return List.copyOf(attributes);
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
        Page input = withRowPositions(raw, blockFactory);
        boolean success = false;
        try {
            template.slot.bind(input);
            var result = new Bound(template.plan, input, template.slot);
            success = true;
            return result;
        } finally {
            if (success == false) {
                input.releaseBlocks();
            }
        }
    }

    private Template template(int branch) {
        if (branch < 0 || branch >= eventSources.size()) {
            throw new IllegalArgumentException("invalid sequence source branch [" + branch + "]");
        }
        Template template = templates[branch];
        if (template == null) {
            EsRelation selected = eventSources.get(branch);
            var slot = new InputSlot();
            var prepared = SequenceStageProjection.prepare(stage, visibleOutput, selected, rowMarker, slot);
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

    private record Template(EsRelation source, PhysicalPlan plan, InputSlot slot, QueryBuilder query) {}

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
        private final PhysicalPlan plan;
        private final Page input;
        private final InputSlot slot;
        private boolean closed;

        private Bound(PhysicalPlan plan, Page input, InputSlot slot) {
            this.plan = plan;
            this.input = input;
            this.slot = slot;
        }

        public PhysicalPlan plan() {
            return plan;
        }

        @Override
        public synchronized void close() {
            if (closed == false) {
                closed = true;
                slot.unbind(input);
                input.releaseBlocks();
            }
        }
    }
}
