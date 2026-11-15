/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.hamcrest.Matchers.containsString;

/** Keeps the command's stage scopes and generated output IDs intact on the wire. */
public class SequenceSerializationTests extends AbstractLogicalPlanSerializationTests<Sequence> {
    @Override
    protected Sequence createTestInstance() {
        List<LogicalPlan> stages = new ArrayList<>();
        List<Attribute> keys = randomBoolean() ? List.of(randomReferenceAttribute(false)) : List.of();
        for (int i = 0; i < between(2, 6); i++) {
            stages.add(new SequenceStage(randomSource(), randomChild(0), keys));
        }
        return new Sequence(randomSource(), stages, randomLongBetween(1, Long.MAX_VALUE));
    }

    @Override
    protected Sequence mutateInstance(Sequence instance) {
        return new Sequence(
            instance.source(),
            instance.children(),
            randomValueOtherThan(instance.maxSpanNanos(), () -> randomLongBetween(1, Long.MAX_VALUE))
        );
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testRejectUnsupportedPeerBeforeWriting() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5));
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> createTestInstance().writeTo(out)).getMessage(),
                containsString("SEQUENCE is not supported")
            );
            assertEquals(0, out.size());
        }
    }

    /** An empty input proves the version check precedes the source header and all child-plan reads. */
    public void testOlderPeerIsRejectedBeforeReadingAnyLogicalPlanBytes() throws Exception {
        for (var entry : List.of(Sequence.ENTRY, SequenceStage.ENTRY, SequenceCondition.ENTRY)) {
            try (var in = StreamInput.wrap(new byte[0])) {
                in.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5));
                assertThat(
                    expectThrows(IllegalArgumentException.class, () -> entry.reader.read(in)).getMessage(),
                    containsString("SEQUENCE is not supported")
                );
                assertEquals(0, in.available());
            }
        }
    }

    /** The wire state contains stages, window and resolved schema, with no public policy or candidate setting. */
    public void testWireContainsOnlyWindowStagesAndSchema() throws Exception {
        Sequence sequence = createTestInstance();
        try (BytesStreamOutput actual = new BytesStreamOutput(); BytesStreamOutput expected = new BytesStreamOutput()) {
            actual.setTransportVersion(TransportVersion.current());
            expected.setTransportVersion(TransportVersion.current());
            sequence.writeTo(new PlanStreamOutput(actual, configuration()));
            var minimal = new PlanStreamOutput(expected, configuration());
            Source.EMPTY.writeTo(minimal);
            minimal.writeNamedWriteableCollection(sequence.children());
            minimal.writeVLong(sequence.maxSpanNanos());
            minimal.writeNamedWriteableCollection(sequence.sourceAttributes());
            minimal.writeCollection(sequence.sourceStages(), org.elasticsearch.common.io.stream.StreamOutput::writeVInt);
            minimal.writeNamedWriteableCollection(sequence.output());
            minimal.writeBoolean(sequence.expressionsResolved());
            assertEquals(expected.bytes(), actual.bytes());
        }
    }

    public void testRegisteredConditionInsideSequenceRoundTripAndEnclosingOldPeerGuards() throws Exception {
        Attribute source = new ReferenceAttribute(Source.EMPTY, null, "flag", DataType.BOOLEAN);
        Attribute reference = new ReferenceAttribute(Source.EMPTY, null, "flag", DataType.BOOLEAN, Nullability.FALSE, new NameId(), true);
        var child = new LocalRelation(Source.EMPTY, List.of(source), EmptyLocalSupplier.EMPTY);
        var condition = new SequenceCondition(
            Source.EMPTY,
            child,
            1,
            reference,
            List.of(0),
            List.of(new SequenceCondition.Input(reference, 0, source))
        );
        var earlier = new SequenceStage(Source.EMPTY, child, List.of());
        var later = new SequenceStage(Source.EMPTY, condition, List.of());
        var sequence = new Sequence(Source.EMPTY, List.of(earlier, later), 1).resolveSchema();
        Sequence restored = copyInstance(sequence);
        assertEquals(sequence, restored);
        assertEquals(condition, restored.collect(SequenceCondition.class).getFirst());
        for (LogicalPlan plan : List.of(sequence, later, condition)) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5));
                assertThat(
                    expectThrows(IllegalArgumentException.class, () -> plan.writeTo(out)).getMessage(),
                    containsString("SEQUENCE is not supported")
                );
                assertEquals(0, out.size());
            }
        }
    }

    public void testConditionPlanRoundTripPreservesOrdinaryPrimaryBindingsAndProvenance() throws Exception {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
        var parsed = EsqlTestUtils.TEST_PARSER.createStatement("""
            SEQUENCE BY key WITHIN 30s PRIMARY issued
            (FROM a | KEEP key,label) AS issued,
            (FROM b | EVAL own=label | WHERE own != MEMBER(issued,label) | KEEP key,label
             | RENAME label AS renamed | DROP renamed) AS signed
            | KEEP key,label,sequence_provenance | LIMIT 10
            """);
        var analyzer = EsqlTestUtils.analyzer().minimumTransportVersion(TransportVersion.current());
        Map<String, EsField> fields = Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "key",
            new EsField("key", DataType.KEYWORD, Map.of(), true, NONE),
            "label",
            new EsField("label", DataType.KEYWORD, Map.of(), true, NONE)
        );
        for (String name : List.of("a", "b")) {
            analyzer.addIndex(new EsIndex(name, fields, Map.of(name, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
        }
        LogicalPlan original = analyzer.buildAnalyzer().analyze(parsed.plan());
        // A full query can contain any expression, including Not from !=; use the same complete plan/expression entries as production.
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(PlanWritables.getNamedWriteables());
        entries.addAll(ExpressionWritables.getNamedWriteables());
        var completeRegistry = new NamedWriteableRegistry(entries);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            new PlanStreamOutput(out, EsqlTestUtils.TEST_CFG).writeNamedWriteable(original);
            try (
                var in = new PlanStreamInput(
                    out.bytes().streamInput(),
                    completeRegistry,
                    EsqlTestUtils.TEST_CFG,
                    new SerializationTestUtils.TestNameIdMapper()
                )
            ) {
                in.setTransportVersion(TransportVersion.current());
                LogicalPlan restored = in.readNamedWriteable(LogicalPlan.class);
                assertEquals(original, restored);
                assertEquals(original.output(), restored.output());
                Sequence sequence = restored.collect(Sequence.class).getFirst();
                assertEquals(1, sequence.sourceAttributes().stream().filter(Sequence::isProvenanceAttribute).count());
                assertTrue(sequence.references().stream().noneMatch(Sequence::isProvenanceAttribute));
                SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
                assertEquals(1, condition.ownStage());
                assertEquals(List.of(0), condition.referencedStages());
                assertTrue(condition.expressionsResolved());
            }
        }
    }

    /** Link constraints round-trip with the full command while older peers reject them before either plan writes bytes. */
    public void testIncomingLinkLimitRoundTripAndUnsupportedPeer() throws Exception {
        Sequence sequence = withIncomingLimit(createTestInstance(), randomLongBetween(1, Long.MAX_VALUE));
        Sequence restored = copyInstance(sequence, TransportVersion.current());
        assertEquals(sequence, restored);
        assertTrue(restored.hasLinkTimeLimits());
        long limit = ((SequenceStage) sequence.children().get(1)).maxGapNanos();
        assertEquals(limit, ((SequenceStage) restored.children().get(1)).maxGapNanos());
        for (LogicalPlan plan : List.of(sequence, sequence.children().get(1))) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5));
                assertThat(
                    expectThrows(IllegalArgumentException.class, () -> plan.writeTo(out)).getMessage(),
                    containsString("SEQUENCE is not supported")
                );
                assertEquals(0, out.size());
            }
        }
    }

    /** Even an unconstrained stage refuses a peer which could otherwise execute the retired rule. */
    public void testUnconstrainedStageRejectsOlderPeerBeforeWriting() throws Exception {
        var stage = (SequenceStage) createTestInstance().children().getFirst();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5));
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> stage.writeTo(out)).getMessage(),
                containsString("SEQUENCE is not supported")
            );
            assertEquals(0, out.size());
        }
    }

    private static Sequence withIncomingLimit(Sequence sequence, long limit) {
        var stages = new ArrayList<>(sequence.children());
        SequenceStage stage = (SequenceStage) stages.get(1);
        stages.set(1, new SequenceStage(stage.source(), stage.child(), stage.keys(), limit));
        return sequence.replaceChildren(stages);
    }

    public void testResolvedWideSchemaRoundTrip() throws Exception {
        List<LogicalPlan> stages = new ArrayList<>();
        for (int stage = 0; stage < 3; stage++) {
            Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
            Attribute payload = new ReferenceAttribute(Source.EMPTY, null, "payload", DataType.LONG);
            var child = new LocalRelation(Source.EMPTY, List.of(key, payload), EmptyLocalSupplier.EMPTY);
            stages.add(new SequenceStage(Source.EMPTY, child, List.of(key)));
        }
        Sequence sequence = new Sequence(Source.EMPTY, stages, 30_000_000_000L).resolveSchema();
        Sequence restored = copyInstance(sequence);
        assertTrue(restored.resolved());
        assertEquals(sequence, restored);
        assertEquals(
            List.of("key", "stage1.payload", "stage2.payload", "stage3.payload"),
            restored.output().subList(0, 4).stream().map(Attribute::name).toList()
        );
        assertEquals(5, restored.output().size());
        assertEquals(5, restored.sourceAttributes().size());
        Attribute marker = restored.sourceAttributes().getLast();
        assertEquals(MetadataAttribute.class, marker.getClass());
        assertEquals("_sequence_provenance", marker.name());
        assertTrue(marker.synthetic());
        assertEquals(DataType.KEYWORD, marker.dataType());
        assertTrue(Sequence.isProvenanceAttribute(marker));
        assertEquals(1, restored.sourceAttributes().stream().filter(Sequence::isProvenanceAttribute).count());
        assertEquals(ReferenceAttribute.class, restored.output().getLast().getClass());
        assertEquals(DataType.KEYWORD, restored.output().getLast().dataType());
        assertFalse(restored.output().getLast().synthetic());
        assertNotEquals(marker.id(), restored.output().getLast().id());
        assertEquals(sequence.output().stream().map(Attribute::id).toList(), restored.output().stream().map(Attribute::id).toList());
        assertEquals(
            sequence.sourceAttributes().stream().map(Attribute::id).toList(),
            restored.sourceAttributes().stream().map(Attribute::id).toList()
        );
        assertEquals(List.of(0, 0, 1, 2, 0), restored.sourceStages());
        assertNotEquals(sequence.sourceAttributes().get(1).id(), sequence.output().get(1).id());
    }

    /** Each schema binding variant survives transport, including resolved and unresolved empty schemas. */
    public void testSchemaFieldVariantsRoundTrip() throws Exception {
        for (Sequence sequence : SequenceTests.schemaFieldVariants()) {
            Sequence restored = copyInstance(sequence);
            assertEquals(sequence, restored);
            assertEquals(sequence.sourceAttributes(), restored.sourceAttributes());
            assertEquals(sequence.sourceStages(), restored.sourceStages());
            assertEquals(sequence.output(), restored.output());
            assertEquals(sequence.expressionsResolved(), restored.expressionsResolved());
        }
    }

    /** The resolved schema remains traversable and retains stage bindings after NodeInfo reconstruction. */
    public void testResolvedSchemaSupportsExpressionAndPropertyTransforms() {
        Sequence sequence = resolvedSequence();
        assertSame(sequence, sequence.transformExpressionsOnly(expression -> expression));
        Sequence changed = (Sequence) sequence.transformPropertiesOnly(Long.class, span -> span + 1);
        assertEquals(sequence.maxSpanNanos() + 1, changed.maxSpanNanos());
        assertEquals(sequence.output(), changed.output());
        assertEquals(sequence.sourceAttributes(), changed.sourceAttributes());
        assertEquals(sequence.sourceStages(), changed.sourceStages());
    }

    private static Sequence resolvedSequence() {
        List<LogicalPlan> stages = new ArrayList<>();
        for (int stage = 0; stage < 2; stage++) {
            Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
            Attribute payload = new ReferenceAttribute(Source.EMPTY, null, "payload", DataType.LONG);
            var input = new LocalRelation(Source.EMPTY, List.of(key, payload), EmptyLocalSupplier.EMPTY);
            stages.add(new SequenceStage(Source.EMPTY, input, List.of(key)));
        }
        return new Sequence(Source.EMPTY, stages, 30_000_000_000L).resolveSchema();
    }
}
