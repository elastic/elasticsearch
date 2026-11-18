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
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
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
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * An ordered, positive-stage sequence producing one wide row per completed match through bounded latest-valid
 * backward search. Union branch limit pushdown through {@link MergePlan} would silently discard valid candidates.
 * Output limits and predicates operate on already selected matches, without changing predecessor selection or backtracking.
 */
public final class Sequence extends LogicalPlan
    implements
        ExecutesOn.Coordinator,
        PostAnalysisVerificationAware,
        PostAnalysisPlanVerificationAware,
        PostOptimizationPlanVerificationAware,
        PostOptimizationVerificationAware.CoordinatorOnly,
        TelemetryAware {
    public static final TransportVersion ESQL_SEQUENCE = TransportVersion.fromName("esql_sequence_repeatable_input");

    /** Structural capacity for parsing and plan decoding. Execution admission is checked separately. */
    public static final int MAX_STAGES = 256;
    public static final int MAX_EXECUTION_STAGES = 4;
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Sequence", Sequence::new);

    private static final String PROVENANCE_ATTRIBUTE = "_sequence_provenance";

    /** A binding belongs to one stage and source identity; diagnostic display names can collide or change. */
    private record SourceBinding(int stage, NameId source) {}

    private final long maxSpanNanos;
    private final List<Attribute> sourceAttributes;
    private final List<Integer> sourceStages;
    private final List<Attribute> output;
    private final boolean schemaResolved;

    /** Keeps the overall window independent of each incoming link's optional bound. */
    public Sequence(Source source, List<LogicalPlan> stages, long maxSpanNanos) {
        this(source, stages, maxSpanNanos, List.of(), List.of(), List.of(), false);
    }

    /** Preserves protected schema bindings through analysis, optimization and transport. */
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
        if (((SequenceStage) stages.getFirst()).maxGapNanos() != 0) {
            throw new IllegalArgumentException("The first SEQUENCE member cannot have an incoming link time limit");
        }
        if (maxSpanNanos <= 0) {
            throw new IllegalArgumentException("SEQUENCE WITHIN duration must be positive");
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

    /** Rejects peers which cannot run the current provenance and profiling contract before any enclosing plan bytes are read or written. */
    public static void checkPlanSupported(TransportVersion version) {
        if (version.supports(ESQL_SEQUENCE) == false) {
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

    /** Conditions remain at their WHERE binding site until execution prepares their mandatory value section. */
    public boolean hasConditions() {
        return children().stream().anyMatch(stage -> stage.anyMatch(plan -> plan instanceof SequenceCondition));
    }

    /** Whether any incoming link has an additional time limit inside the overall window. */
    public boolean hasLinkTimeLimits() {
        for (int stage = 1; stage < children().size(); stage++) {
            if (((SequenceStage) children().get(stage)).maxGapNanos() > 0) {
                return true;
            }
        }
        return false;
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
        var references = AttributeSet.of(sourceAttributes.stream().filter(attribute -> isProvenanceAttribute(attribute) == false).toList())
            .asBuilder();
        for (SequenceCondition condition : collect(SequenceCondition.class)) {
            condition.activeInputs()
                .stream()
                .filter(input -> input.stage() != condition.ownStage())
                .forEach(input -> references.add(input.source()));
        }
        return references.build();
    }

    @Override
    public boolean expressionsResolved() {
        return schemaResolved;
    }

    /** Refreshes the wide schema after child resolution, preserving output IDs through fixpoint analysis and plan rewrites. */
    public Sequence resolveSchema() {
        Sequence bound = resolveConditions();
        if (bound != this) {
            return bound.resolveSchema();
        }
        if (childrenResolved() == false) {
            return this;
        }
        List<Attribute> inputs = new ArrayList<>();
        List<Integer> stages = new ArrayList<>();
        Map<SourceBinding, Attribute> previous = new HashMap<>();
        for (int column = 0; column < sourceAttributes.size(); column++) {
            previous.put(new SourceBinding(sourceStages.get(column), sourceAttributes.get(column).id()), output.get(column));
        }
        SequenceStage first = (SequenceStage) children().getFirst();
        List<Attribute> columns = new ArrayList<>();
        for (Attribute key : first.keys()) {
            columns.add(reference(key, 0, key.name(), previous));
            inputs.add(key);
            stages.add(0);
        }
        Set<String> names = new HashSet<>();
        columns.forEach(column -> names.add(column.name()));
        for (int stage = 0; stage < children().size(); stage++) {
            SequenceStage child = (SequenceStage) children().get(stage);
            Set<Attribute> keys = new HashSet<>(child.keys());
            for (Attribute attribute : child.output()) {
                if (keys.contains(attribute) == false) {
                    String base = "stage" + (stage + 1) + "." + attribute.name();
                    String name = base;
                    int suffix = 1;
                    while (names.add(name) == false)
                        name = base + "_" + suffix++;
                    columns.add(reference(attribute, stage, name, previous));
                    inputs.add(attribute);
                    stages.add(stage);
                }
            }
        }
        Attribute provenance = null;
        Attribute previousProvenance = null;
        for (int column = 0; column < sourceAttributes.size(); column++) {
            if (isProvenanceAttribute(sourceAttributes.get(column))) {
                provenance = sourceAttributes.get(column);
                previousProvenance = output.get(column);
                break;
            }
        }
        if (provenance == null) {
            provenance = new MetadataAttribute(Source.EMPTY, PROVENANCE_ATTRIBUTE, DataType.KEYWORD, Nullability.FALSE, null, true, false);
        }
        String base = Attribute.rawTemporaryName("sequence_provenance", provenance.id().toString());
        String name = base;
        int suffix = 1;
        while (names.add(name) == false)
            name = base + "_" + suffix++;
        inputs.add(provenance);
        stages.add(0);
        columns.add(
            new ReferenceAttribute(
                Source.EMPTY,
                null,
                name,
                DataType.KEYWORD,
                Nullability.FALSE,
                previousProvenance == null ? null : previousProvenance.id(),
                false
            )
        );
        if (schemaResolved && sourceAttributes.equals(inputs) && sourceStages.equals(stages) && output.equals(columns)) {
            return this;
        }
        return new Sequence(source(), children(), maxSpanNanos, inputs, stages, columns, true);
    }

    /** Only compiler-owned metadata can request match identities; public names never establish this output role. */
    public static boolean isProvenanceAttribute(Attribute attribute) {
        return attribute instanceof MetadataAttribute
            && attribute.synthetic()
            && attribute.name().equals(PROVENANCE_ATTRIBUTE)
            && attribute.dataType() == DataType.KEYWORD;
    }

    /** Resolves backward demands before requiring every stage to resolve, avoiding a cross-stage analysis deadlock. */
    public Sequence resolveConditions() {
        List<LogicalPlan> stages = new ArrayList<>(children());
        boolean changed = false;
        for (int stage = 0; stage < stages.size(); stage++) {
            SequenceStage original = (SequenceStage) stages.get(stage);
            LogicalPlan resolved = original.child().transformUp(SequenceCondition.class, condition -> condition.resolve(stages));
            if (resolved != original.child()) {
                stages.set(stage, original.replaceChild(resolved));
                changed = true;
            }
        }
        return changed ? replaceChildren(stages) : this;
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

    private Attribute reference(Attribute source, int stage, String name, Map<SourceBinding, Attribute> previous) {
        Attribute existing = previous.get(new SourceBinding(stage, source.id()));
        NameId id = existing == null ? null : existing.id();
        return SequenceSchema.reference(source, source.source(), name, id, false, children().get(stage));
    }

    /** Check full-text use before optimization can discard the mapped producer behind a materialized output. */
    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (plan, failures) -> SequenceSchema.verifyFullText(this, plan, failures);
    }

    /** Alias rewrites must retain the same materialized-column analyzer restriction. */
    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        return (plan, failures) -> SequenceSchema.verifyFullText(this, plan, failures);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        SequenceStage first = (SequenceStage) children().getFirst();
        for (int stage = 0; stage < children().size(); stage++) {
            for (SequenceCondition condition : children().get(stage).collect(SequenceCondition.class)) {
                if (condition.ownStage() != stage) {
                    failures.add(
                        fail(condition, "SEQUENCE condition owner [{}] does not match stage [{}]", condition.ownStage() + 1, stage + 1)
                    );
                }
            }
        }
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
            List<Attribute> mandatory = new ArrayList<>();
            for (int column = 0; column < sourceAttributes.size(); column++) {
                if (sourceStages.get(column) == index && isProvenanceAttribute(sourceAttributes.get(column)) == false) {
                    payloads.add(sourceAttributes.get(column));
                }
            }
            for (SequenceCondition condition : collect(SequenceCondition.class)) {
                for (SequenceCondition.Input input : condition.activeInputs()) {
                    if (input.stage() == index && input.stage() != condition.ownStage() && payloads.contains(input.source()) == false) {
                        List<Attribute> canonical = stage.output()
                            .stream()
                            .filter(attribute -> attribute.id().equals(input.source().id()))
                            .toList();
                        if (canonical.size() != 1 || canonical.getFirst().dataType() != input.source().dataType()) {
                            failures.add(
                                fail(condition, "SEQUENCE stage [{}] lost condition column [{}]", index + 1, input.source().name())
                            );
                            continue;
                        }
                        payloads.add(canonical.getFirst());
                    } else if (input.stage() == index && input.stage() == condition.ownStage()) {
                        List<Attribute> canonical = condition.child()
                            .output()
                            .stream()
                            .filter(attribute -> attribute.id().equals(input.source().id()))
                            .toList();
                        if (canonical.size() != 1 || canonical.getFirst().dataType() != input.source().dataType()) {
                            failures.add(
                                fail(condition, "SEQUENCE stage [{}] lost own condition column [{}]", index + 1, input.source().name())
                            );
                            continue;
                        }
                        if (mandatory.stream().noneMatch(attribute -> attribute.id().equals(canonical.getFirst().id()))) {
                            mandatory.add(canonical.getFirst());
                        }
                    }
                }
            }
            for (EsRelation source : SequenceStageProjection.eventSources(stage)) {
                var prepared = SequenceStageProjection.prepare(stage, payloads, mandatory, source, marker, EmptyLocalSupplier.EMPTY);
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
                                    "SEQUENCE stage [{}] cannot read source column [{}] of type [{}]; "
                                        + "DROP the unused column inside the stage or use final KEEP to select the required columns",
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
