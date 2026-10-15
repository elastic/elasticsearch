/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/** Verifies independent stage scopes, the wide schema, and candidate completeness boundaries. */
public class SequenceAnalyzerTests extends ESTestCase {
    public void testTypedWideOutputAndPostSequenceFilter() {
        var plan = configured().query("""
            sequence by host.id with {"maxspan":"30s"}
            (from a | eval label = "network" | keep host.id, @timestamp, label),
            (from b | eval label = "process" | keep host.id, @timestamp, label)
            | where stage2.@timestamp >= stage1.@timestamp
            """);
        Sequence sequence = plan.collect(Sequence.class).getFirst();
        assertEquals(
            List.of("host.id", "stage1.@timestamp", "stage1.label", "stage2.@timestamp", "stage2.label"),
            sequence.output().stream().map(Attribute::name).toList()
        );
        assertEquals(
            List.of(DataType.KEYWORD, DataType.DATETIME, DataType.KEYWORD, DataType.DATETIME, DataType.KEYWORD),
            sequence.output().stream().map(Attribute::dataType).toList()
        );
        assertTrue(sequence.childrenResolved());
        assertEquals(1, plan.collect(Limit.class).size());
        for (var stage : sequence.children()) {
            assertTrue(stage.collect(Limit.class).isEmpty());
        }
        assertSame(sequence, sequence.resolveSchema());
        assertEquals(sequence.output(), sequence.replaceChildren(sequence.children()).output());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testDerivedExpandedKeysResolveInEachStage() {
        var plan = configured().query("""
            sequence by commonkey with {"maxspan":"2m"}
            (from a | eval commonkey = TO_LOWER(host.id) | mv_expand commonkey | keep commonkey, label),
            (from b | eval commonkey = TO_LOWER(label) | mv_expand commonkey | keep commonkey, label)
            | keep commonkey, stage1.label, stage2.label
            | limit 4
            """);
        Sequence sequence = plan.collect(Sequence.class).getFirst();
        var first = (SequenceStage) sequence.children().getFirst();
        var second = (SequenceStage) sequence.children().get(1);
        assertTrue(first.keys().getFirst().resolved());
        assertTrue(second.keys().getFirst().resolved());
        assertNotEquals(first.keys().getFirst().id(), second.keys().getFirst().id());
        assertEquals(List.of("commonkey", "stage1.label", "stage2.label"), sequence.output().stream().map(Attribute::name).toList());
        assertTrue(first.child().collect(Limit.class).isEmpty());
        assertTrue(second.child().collect(Limit.class).isEmpty());
    }

    public void testKeysMustSurviveProjection() {
        configured().error("""
            sequence by host.id with {"maxspan":"30s"}
            (from a | keep label), (from b | keep host.id)
            """, containsString("Unknown column [host.id]"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testIndexedSearchRequiresAnExplicitStageBoundary() {
        for (String predicate : List.of("MATCH(label, \"term\")", "QSTR(\"label:term\")", "KQL(\"label:term\")")) {
            configured().error(Strings.format("""
                sequence by host.id with {"maxspan":"30s"}
                (from a | where %s | keep host.id), (from b | keep host.id) | limit 1
                """, predicate), containsString("is not supported inside a SEQUENCE stage"));
        }
    }

    public void testViewStagesRequireTheirOwnRequestFilterScope() {
        configured().addView("computed_events", "FROM a | EVAL computed = \"yes\"").error("""
            sequence by host.id with {"maxspan":"30s"}
            (from computed_events,a | keep host.id, computed), (from b | keep host.id)
            | limit 1
            """, containsString("Views are not supported inside a SEQUENCE stage"));
    }

    public void testCrossClusterEventStageIsRejectedDuringVerification() {
        var remote = new EsIndex(
            "remote:logs",
            mapping(),
            Map.of("remote:logs", new IndexProperties(IndexMode.STANDARD, 1)),
            Map.of("remote", List.of("logs")),
            Map.of("remote", List.of("logs"))
        );
        String message = configured().addIndex(remote).error("""
            sequence by host.id with {"maxspan":"30s"}
            (from remote:logs | keep host.id), (from b | keep host.id)
            | limit 1
            """, containsString("Cross-cluster sources are not supported inside a SEQUENCE stage"));
        assertThat(message, containsString("line 2:"));
    }

    public void testConflictingKeyTypes() {
        configured().error("""
            sequence by commonkey with {"maxspan":"30s"}
            (from a | eval commonkey = host.id),
            (from b | eval commonkey = 1)
            """, containsString("SEQUENCE BY column [commonkey] has conflicting types"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testSourceTimestampRequiredDespiteProjection() {
        configured().addIndex(index("c", Map.of("host", host()))).error("""
            sequence by host.id with {"maxspan":"30s"} (from a | keep host.id), (from c | keep host.id)
            """, containsString("require a source [@timestamp]"));
        configured().error("""
            sequence by host.id with {"maxspan":"30s"}
            (from a | eval @timestamp = NOW()), (from b)
            """, containsString("cannot overwrite the source [@timestamp]"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testEmptyKeyAndThreeStages() {
        var plan = configured().query("""
            sequence with {"maxspan":"30s"}
            (from a | keep label), (from b | keep label), (from a | keep label)
            """);
        assertEquals(List.of("stage1.label", "stage2.label", "stage3.label"), plan.output().stream().map(Attribute::name).toList());
        assertEquals(3, plan.collect(Sequence.class).getFirst().children().size());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testOldClusterRejectedBeforePlanning() {
        configured().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_STAGE_COLUMNS)).error("""
            sequence by host.id with {"maxspan":"30s"} (from a), (from b)
            """, containsString("SEQUENCE is not supported by all nodes in the cluster"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testOptimizerKeepsCompletedRowLimitsOutsideStages() {
        var analyzed = configured().query("""
            sequence by host.id with {"maxspan":"30s"}
            (from a | eval transformed = TO_LOWER(label) | keep host.id, transformed),
            (from b | eval transformed = TO_LOWER(label) | keep host.id, transformed)
            | where stage1.transformed != stage2.transformed
            | sort stage2.transformed
            | limit 1 by host.id
            | limit 3
            """);
        var optimizer = new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        );
        var optimized = optimizer.optimize(analyzed);
        Sequence sequence = optimized.collect(Sequence.class).getFirst();
        assertEquals(analyzed.output(), optimized.output());
        for (var stage : sequence.children()) {
            assertTrue(stage.collect(Limit.class).isEmpty());
            assertTrue(stage.collect(TopN.class).isEmpty());
            assertTrue(stage.collect(TopNBy.class).isEmpty());
        }
    }

    public void testDateNanosSource() {
        var nanosMapping = new HashMap<>(mapping());
        nanosMapping.put("@timestamp", new EsField("@timestamp", DataType.DATE_NANOS, Map.of(), true, NONE));
        var plan = configured().addIndex(index("nanos", nanosMapping)).query("""
            sequence by host.id with {"maxspan":"1nanos"}
            (from nanos | keep host.id, @timestamp), (from nanos | keep host.id, @timestamp)
            | limit 3
            """);
        assertEquals(
            List.of(DataType.KEYWORD, DataType.DATE_NANOS, DataType.DATE_NANOS),
            plan.output().stream().map(Attribute::dataType).toList()
        );
    }

    public void testRenameCannotOverwriteTimestamp() {
        for (String stage : List.of(
            "from a | rename label as @timestamp",
            "from a | drop @timestamp | rename label as @timestamp",
            "from a | rename label as time | rename time as @timestamp"
        )) {
            configured().error(
                "sequence by host.id with {\"maxspan\":\"30s\"} (" + stage + "), (from b) | limit 1",
                containsString("cannot overwrite the source [@timestamp]")
            );
        }
        var restored = configured().query("""
            sequence by host.id with {"maxspan":"30s"}
            (from a | rename @timestamp as original | rename original as @timestamp), (from b)
            | limit 1
            """);
        assertEquals(
            DataType.DATETIME,
            restored.output().stream().filter(a -> a.name().equals("stage1.@timestamp")).findFirst().orElseThrow().dataType()
        );
    }

    public void testTimestampRenameThroughUnion() {
        var restored = configured().query("""
            sequence by host.id with {"maxspan":"30s"}
            (from (from a), (from b) | rename @timestamp as original | rename original as @timestamp), (from a)
            | limit 1
            """);
        assertEquals(
            DataType.DATETIME,
            restored.output().stream().filter(a -> a.name().equals("stage1.@timestamp")).findFirst().orElseThrow().dataType()
        );
        configured().error("""
            sequence by host.id with {"maxspan":"30s"}
            (from (from a), (from b | eval other_time = @timestamp + 1 second | rename other_time as @timestamp)), (from a)
            | limit 1
            """, containsString("cannot overwrite the source [@timestamp]"));
    }

    public void testUnusedStagePayloadColumnsArePruned() {
        var analyzed = configured().query("""
            sequence by host.id with {"maxspan":"30s"}
            (from a | eval unused = TO_LOWER(label)), (from b | eval unused = TO_LOWER(label))
            | keep host.id
            | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        Sequence sequence = optimized.collect(Sequence.class).getFirst();
        assertEquals(List.of("host.id"), sequence.output().stream().map(Attribute::name).toList());
        assertEquals(1, sequence.sourceAttributes().size());
        assertEquals(List.of(0), sequence.sourceStages());
        assertTrue(sequence.collect(Eval.class).isEmpty());
        for (var stage : sequence.children()) {
            assertTrue(((SequenceStage) stage).keys().getFirst().resolved());
        }
    }

    public void testEmptyStageProducesEmptyWideResult() {
        for (int emptyStage = 0; emptyStage < 3; emptyStage++) {
            List<String> stages = new java.util.ArrayList<>();
            for (int stage = 0; stage < 3; stage++) {
                stages.add("(from a" + (stage == emptyStage ? " | where false" : "") + " | keep host.id, label)");
            }
            var analyzed = configured().query("sequence by host.id with {\"maxspan\":\"30s\"} " + String.join(", ", stages) + " | limit 3");
            var optimized = optimizer().optimize(analyzed);
            assertEquals(analyzed.output(), optimized.output());
            assertTrue(optimized.collect(Sequence.class).isEmpty());
            assertTrue(optimized.collect(LocalRelation.class).stream().anyMatch(LocalRelation::hasEmptySupplier));
        }
    }

    public void testKeylessCountRetainsEventStages() {
        var analyzed = configured().query("""
            sequence with {"maxspan":"30s"} (from a), (from b)
            | stats count(*)
            | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        Sequence sequence = optimized.collect(Sequence.class).getFirst();
        assertTrue(sequence.output().isEmpty());
        assertEquals(2, sequence.children().size());
        assertFalse(sequence.collect(org.elasticsearch.xpack.esql.plan.logical.EsRelation.class).isEmpty());
    }

    public void testRequiredRawSourceTypesAreRejectedAfterOptimization() {
        for (DataType type : List.of(
            DataType.GEO_POINT,
            DataType.GEO_SHAPE,
            DataType.CARTESIAN_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.DENSE_VECTOR,
            DataType.TSID_DATA_TYPE,
            DataType.DATE_RANGE,
            DataType.DOUBLE_RANGE
        )) {
            var mapping = new HashMap<>(mapping());
            mapping.put("special", new EsField("special", type, Map.of(), true, NONE));
            var analyzed = configured().addIndex(index("special_events", mapping)).query("""
                sequence by host.id with {"maxspan":"30s"} (from special_events), (from b)
                | keep host.id, stage1.special | limit 3
                """);
            var error = expectThrows(VerificationException.class, () -> optimizer().optimize(analyzed));
            assertThat(
                error.getMessage(),
                containsString("SEQUENCE stage [1] cannot read source column [special] of type [" + type.typeName() + "]")
            );
        }
    }

    public void testUnusedUnsupportedReaderFieldsCanBePruned() {
        var mapping = new HashMap<>(mapping());
        mapping.put("location", new EsField("location", DataType.GEO_POINT, Map.of(), true, NONE));
        for (String stage : List.of(
            "from spatial",
            "from spatial | eval unused=TO_STRING(location)",
            "from (from spatial), (from spatial)"
        )) {
            var analyzed = configured().addIndex(index("spatial", mapping))
                .query("sequence by host.id with {\"maxspan\":\"30s\"} (" + stage + "), (from b) | keep host.id | limit 3");
            var optimized = optimizer().optimize(analyzed);
            assertEquals(List.of("host.id"), optimized.output().stream().map(Attribute::name).toList());
        }
    }

    public void testRequiredMixedMappingConversionIsRejectedAfterOptimization() {
        var mapping = mixedMapping();
        var analyzed = configured().addIndex(index("mixed_events", mapping)).query("""
            sequence by host.id with {"maxspan":"30s"} (from mixed_events), (from b)
            | keep host.id, stage1.mixed | limit 3
            """);
        assertThat(
            expectThrows(VerificationException.class, () -> optimizer().optimize(analyzed)).getMessage(),
            containsString("cannot read source column [mixed] requiring a conversion across mappings")
        );
    }

    public void testUnusedMixedMappingConversionCanBePruned() {
        for (String stage : List.of("from mixed_events", "from mixed_events | eval unused=mixed+1")) {
            var analyzed = configured().addIndex(index("mixed_events", mixedMapping()))
                .query("sequence by host.id with {\"maxspan\":\"30s\"} (" + stage + "), (from b) | keep host.id | limit 3");
            assertEquals(List.of("host.id"), optimizer().optimize(analyzed).output().stream().map(Attribute::name).toList());
        }
    }

    private static Map<String, EsField> mixedMapping() {
        var mapping = new HashMap<>(mapping());
        var raw = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "mixed",
            new EsField("mixed", DataType.KEYWORD, Map.of(), true, NONE),
            false
        );
        mapping.put(
            "mixed",
            new CompactMultiTypeEsField("mixed", DataType.LONG, false, Map.of(DataType.KEYWORD, new ToLong(Source.EMPTY, raw)), NONE, null)
        );
        return mapping;
    }

    public void testUnsupportedReaderFieldsUsedByStagePredicatesAndKeysAreRejected() {
        var mapping = new HashMap<>(mapping());
        mapping.put("location", new EsField("location", DataType.GEO_POINT, Map.of(), true, NONE));
        for (String query : List.of("""
            sequence by host.id with {"maxspan":"30s"}
            (from spatial | where ST_X(location)>0 | keep host.id), (from b | keep host.id)
            | keep host.id | limit 3
            """, """
            sequence by normalized with {"maxspan":"30s"}
            (from spatial | eval normalized=TO_STRING(location) | keep normalized),
            (from b | eval normalized=label | keep normalized)
            | stats count(*) | limit 3
            """)) {
            var analyzed = configured().addIndex(index("spatial", mapping)).query(query);
            assertThat(
                expectThrows(VerificationException.class, () -> optimizer().optimize(analyzed)).getMessage(),
                containsString("cannot read source column [location] of type [geo_point]")
            );
        }
    }

    public void testMembershipRightSideKeepsOrdinarySourceTypeSupport() {
        var mapping = new HashMap<>(mapping());
        mapping.put("location", new EsField("location", DataType.GEO_POINT, Map.of(), true, NONE));
        var analyzed = configured().addIndex(index("spatial", mapping)).query("""
            sequence by host.id with {"maxspan":"30s"}
            (from a | where host.id IN (from spatial | where ST_X(location)>0 | keep host.id) | keep host.id),
            (from b | keep host.id) | keep host.id | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        assertEquals(List.of("host.id"), optimized.output().stream().map(Attribute::name).toList());
    }

    public void testPhysicalPlanningKeepsStagesBehindCoordinatorSource() {
        var analyzed = configured().query("""
            sequence by host.id with {"maxspan":"30s"}
            (from a | eval normalized = TO_LOWER(label)), (from b | eval normalized = TO_LOWER(label))
            | where stage1.normalized != stage2.normalized
            | sort stage2.normalized
            | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        var physical = new org.elasticsearch.xpack.esql.planner.mapper.Mapper().map(
            new org.elasticsearch.xpack.esql.session.Versioned<>(optimized, TransportVersion.current())
        );
        var source = physical.collect(org.elasticsearch.xpack.esql.plan.physical.SequenceExec.class).getFirst();
        assertEquals(optimized.collect(Sequence.class).getFirst(), source.sequence());
        assertEquals(analyzed.output(), physical.output());
        assertTrue(physical.collect(org.elasticsearch.xpack.esql.plan.physical.FragmentExec.class).isEmpty());
        assertTrue(physical.collect(org.elasticsearch.xpack.esql.plan.physical.MergeExec.class).isEmpty());
        assertTrue(source.children().isEmpty());
        assertTrue(source.references().isEmpty());
        assertEquals(2, source.sequence().children().size());
        for (var stage : source.sequence().children()) {
            assertTrue(stage.collect(Limit.class).isEmpty());
            assertTrue(stage.collect(TopN.class).isEmpty());
        }
    }

    private LogicalPlanOptimizer optimizer() {
        return new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        );
    }

    private TestAnalyzer configured() {
        return analyzer().minimumTransportVersion(TransportVersion.current())
            .addIndex(index("a", mapping()))
            .addIndex(index("b", mapping()));
    }

    private static Map<String, EsField> mapping() {
        return Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "host",
            host(),
            "label",
            new EsField("label", DataType.KEYWORD, Map.of(), true, NONE)
        );
    }

    private static EsField host() {
        return new EsField("host", DataType.OBJECT, Map.of("id", new EsField("id", DataType.KEYWORD, Map.of(), true, NONE)), false, NONE);
    }

    private static EsIndex index(String name, Map<String, EsField> mapping) {
        return new EsIndex(name, mapping, Map.of(name, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of());
    }
}
