/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.SequenceSourceTypes;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.core.type.UnmappedEsField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * An ordered, positive-stage EQL sequence producing one wide row per completed match. This is deliberately separate
 * from {@link MergePlan}: union branch limit pushdown would silently discard valid candidates. Output limits and
 * predicates operate on already selected matches, without changing predecessor selection or backtracking.
 */
public final class Sequence extends LogicalPlan
    implements
        ExecutesOn.Coordinator,
        PostAnalysisVerificationAware,
        PostOptimizationVerificationAware.CoordinatorOnly,
        TelemetryAware {
    public static final TransportVersion ESQL_SEQUENCE = TransportVersion.fromName("esql_sequence");
    /** The stage-column-only prototype uses a distinct wire layout from earlier experimental builds. */
    public static final TransportVersion ESQL_SEQUENCE_STAGE_COLUMNS = TransportVersion.fromName("esql_sequence_stage_columns");
    /** Matches EQL's syntax capacity; candidate cardinality is governed separately by paging and the request breaker. */
    public static final int MAX_STAGES = 256;
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Sequence", Sequence::new);

    private final long maxSpanNanos;
    private final List<Attribute> sourceAttributes;
    private final List<Integer> sourceStages;
    private final List<Attribute> output;
    private final boolean schemaResolved;

    public Sequence(Source source, List<LogicalPlan> stages, long maxSpanNanos) {
        this(source, stages, maxSpanNanos, List.of(), List.of(), List.of(), false);
    }

    public Sequence(
        Source source,
        List<LogicalPlan> stages,
        long maxSpanNanos,
        List<Attribute> sourceAttributes,
        List<Integer> sourceStages,
        List<Attribute> output,
        boolean schemaResolved
    ) {
        super(source, List.copyOf(stages));
        if (stages.size() < 2 || stages.size() > MAX_STAGES || stages.stream().anyMatch(p -> p instanceof SequenceStage == false)) {
            throw new IllegalArgumentException("a sequence requires between 2 and " + MAX_STAGES + " SequenceStage children");
        }
        if (maxSpanNanos <= 0) {
            throw new IllegalArgumentException("sequence maxspan must be positive");
        }
        this.maxSpanNanos = maxSpanNanos;
        this.sourceAttributes = List.copyOf(sourceAttributes);
        this.sourceStages = List.copyOf(sourceStages);
        this.output = List.copyOf(output);
        this.schemaResolved = schemaResolved;
        if (sourceAttributes.size() != output.size()
            || sourceStages.size() != output.size()
            || sourceStages.stream().anyMatch(stage -> stage < 0 || stage >= stages.size())) {
            throw new IllegalArgumentException("sequence output bindings must align with columns and refer to existing stages");
        }
    }

    private Sequence(StreamInput in) throws IOException {
        this(
            readSource(in),
            in.readNamedWriteableCollectionAsList(LogicalPlan.class),
            in.readVLong(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readCollectionAsList(StreamInput::readVInt),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean()
        );
    }

    private static Source readSource(StreamInput in) throws IOException {
        checkPlanSupported(in.getTransportVersion());
        return Source.readFrom((PlanStreamInput) in);
    }

    /** Fails before writing a partial plan to peers which cannot execute this command. */
    public static void checkSupported(TransportVersion version) {
        if (version.supports(ESQL_SEQUENCE) == false) {
            throw new IllegalArgumentException("SEQUENCE is not supported in transport version [" + version + "]");
        }
    }

    /** Rejects the earlier experimental logical-plan layout before any plan bytes are read or written. */
    public static void checkPlanSupported(TransportVersion version) {
        if (version.supports(ESQL_SEQUENCE_STAGE_COLUMNS) == false) {
            throw new IllegalArgumentException("SEQUENCE is not supported in transport version [" + version + "]");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkPlanSupported(out.getTransportVersion());
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(children());
        out.writeVLong(maxSpanNanos);
        out.writeNamedWriteableCollection(sourceAttributes);
        out.writeCollection(sourceStages, StreamOutput::writeVInt);
        out.writeNamedWriteableCollection(output);
        out.writeBoolean(schemaResolved);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Sequence> info() {
        return NodeInfo.create(this, Sequence::new, children(), maxSpanNanos, sourceAttributes, sourceStages, output, schemaResolved);
    }

    @Override
    public Sequence replaceChildren(List<LogicalPlan> children) {
        return new Sequence(source(), children, maxSpanNanos, sourceAttributes, sourceStages, output, schemaResolved);
    }

    public long maxSpanNanos() {
        return maxSpanNanos;
    }

    public List<Attribute> sourceAttributes() {
        return sourceAttributes;
    }

    public List<Integer> sourceStages() {
        return sourceStages;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(sourceAttributes);
    }

    @Override
    public boolean expressionsResolved() {
        return schemaResolved;
    }

    /** Refreshes the wide schema after child resolution, preserving output IDs through fixpoint analysis and plan rewrites. */
    public Sequence resolveSchema() {
        if (childrenResolved() == false) {
            return this;
        }
        List<Attribute> inputs = new ArrayList<>();
        List<Integer> stages = new ArrayList<>();
        Map<String, Attribute> previous = new HashMap<>();
        for (Attribute attribute : output) {
            previous.put(attribute.name(), attribute);
        }
        SequenceStage first = (SequenceStage) children().getFirst();
        List<Attribute> columns = new ArrayList<>();
        for (Attribute key : first.keys()) {
            columns.add(reference(key, key.name(), previous));
            inputs.add(key);
            stages.add(0);
        }
        for (int stage = 0; stage < children().size(); stage++) {
            SequenceStage child = (SequenceStage) children().get(stage);
            Set<Attribute> keys = new HashSet<>(child.keys());
            for (Attribute attribute : child.output()) {
                if (keys.contains(attribute) == false) {
                    columns.add(reference(attribute, "stage" + (stage + 1) + "." + attribute.name(), previous));
                    inputs.add(attribute);
                    stages.add(stage);
                }
            }
        }
        if (schemaResolved && sourceAttributes.equals(inputs) && sourceStages.equals(stages) && output.equals(columns)) {
            return this;
        }
        return new Sequence(source(), children(), maxSpanNanos, inputs, stages, columns, true);
    }

    /** Prunes payload bindings only. Each stage still retains its own keys and source event identity for matching. */
    public Sequence pruneOutput(AttributeSet used) {
        List<Attribute> inputs = new ArrayList<>();
        List<Integer> stages = new ArrayList<>();
        List<Attribute> columns = new ArrayList<>();
        for (int column = 0; column < sourceAttributes.size(); column++) {
            if (used.contains(output.get(column))) {
                inputs.add(sourceAttributes.get(column));
                stages.add(sourceStages.get(column));
                columns.add(output.get(column));
            }
        }
        return columns.size() == output.size()
            ? this
            : new Sequence(source(), children(), maxSpanNanos, inputs, stages, columns, schemaResolved);
    }

    private static ReferenceAttribute reference(Attribute source, String name, Map<String, Attribute> previous) {
        Attribute existing = previous.get(name);
        NameId id = existing == null ? null : existing.id();
        return new ReferenceAttribute(source.source(), null, name, source.dataType(), source.nullable(), id, false);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        SequenceStage first = (SequenceStage) children().getFirst();
        for (LogicalPlan plan : children()) {
            SequenceStage stage = (SequenceStage) plan;
            if (stage.keys().size() != first.keys().size()) {
                failures.add(fail(stage, "All SEQUENCE stages must have the same BY columns"));
                continue;
            }
            for (int key = 0; key < first.keys().size(); key++) {
                Attribute expected = first.keys().get(key);
                Attribute actual = stage.keys().get(key);
                if (actual.dataType() != expected.dataType()) {
                    failures.add(
                        fail(
                            actual,
                            "SEQUENCE BY column [{}] has conflicting types [{}] and [{}]",
                            actual.name(),
                            expected.dataType(),
                            actual.dataType()
                        )
                    );
                }
            }
        }
        Set<String> names = new HashSet<>();
        for (Attribute attribute : output) {
            if (names.add(attribute.name()) == false) {
                failures.add(fail(attribute, "SEQUENCE output column [{}] is ambiguous", attribute.name()));
            }
        }
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        Attribute marker = new ReferenceAttribute(Source.EMPTY, null, "sequence_source_row", DataType.LONG, Nullability.FALSE, null, true);
        for (int index = 0; index < children().size(); index++) {
            SequenceStage stage = (SequenceStage) children().get(index);
            List<Attribute> payloads = new ArrayList<>();
            for (int column = 0; column < sourceAttributes.size(); column++) {
                if (sourceStages.get(column) == index) {
                    payloads.add(sourceAttributes.get(column));
                }
            }
            for (EsRelation source : SequenceStageProjection.eventSources(stage)) {
                var prepared = SequenceStageProjection.prepare(stage, payloads, source, marker, EmptyLocalSupplier.EMPTY);
                for (Attribute attribute : prepared.source().output()) {
                    if (attribute instanceof UnmappedFieldsAttribute
                        || attribute instanceof FieldAttribute field
                            && field.field() instanceof UnmappedEsField
                            && SequenceSourceTypes.supports(field.field()) == false) {
                        failures.add(
                            fail(
                                stage,
                                "SEQUENCE stage [{}] cannot load unmapped source column [{}]; use mapped source fields",
                                index + 1,
                                attribute.name()
                            )
                        );
                    } else if (attribute instanceof FieldAttribute field
                        && field.field() instanceof UnionTypeEsField
                        && SequenceSourceTypes.supports(field.field()) == false) {
                            failures.add(
                                fail(
                                    stage,
                                    "SEQUENCE stage [{}] cannot read source column [{}] requiring a conversion across mappings; "
                                        + "use consistently mapped source fields",
                                    index + 1,
                                    attribute.name()
                                )
                            );
                        } else if (SequenceSourceTypes.supports(attribute.dataType()) == false) {
                            failures.add(
                                fail(
                                    stage,
                                    "SEQUENCE stage [{}] cannot read source column [{}] of type [{}]",
                                    index + 1,
                                    attribute.name(),
                                    attribute.dataType().typeName()
                                )
                            );
                        }
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(children(), maxSpanNanos, sourceAttributes, sourceStages, output, schemaResolved);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
            || obj instanceof Sequence other
                && children().equals(other.children())
                && maxSpanNanos == other.maxSpanNanos
                && sourceAttributes.equals(other.sourceAttributes)
                && sourceStages.equals(other.sourceStages)
                && output.equals(other.output)
                && schemaResolved == other.schemaResolved;
    }
}
