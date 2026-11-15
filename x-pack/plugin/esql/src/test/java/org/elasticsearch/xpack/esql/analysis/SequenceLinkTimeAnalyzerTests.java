/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/** Verifies feature transport gating independently of the new grammar's surface spelling. */
public class SequenceLinkTimeAnalyzerTests extends ESTestCase {
    private static final String QUERY = """
        SEQUENCE BY key WITHIN 1m PRIMARY 1 (FROM a), (FROM b)
        | KEEP key | LIMIT 10
        """;

    /** These fixtures still enter through the build's default parser, using the existing positional form. */
    @BeforeClass
    public static void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
    }

    /** Every query requires the sole engine, including a query with no incoming limit. */
    public void testIncomingLimitRequiresSupportingCluster() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5);
        LogicalPlan unconstrained = configured(TransportVersion.current()).statement(QUERY);
        assertFalse(unconstrained.collect(Sequence.class).getFirst().hasLinkTimeLimits());
        LogicalPlan constrained = configured(TransportVersion.current()).buildAnalyzer().analyze(withLimit());
        assertTrue(constrained.collect(Sequence.class).getFirst().hasLinkTimeLimits());
        assertEquals(
            unconstrained.output().stream().map(Attribute::name).toList(),
            constrained.output().stream().map(Attribute::name).toList()
        );
        configured(older).statementError(QUERY, containsString("SEQUENCE is not supported by all nodes in the cluster"));
        VerificationException failure = expectThrows(
            VerificationException.class,
            () -> configured(older).buildAnalyzer().analyze(withLimit())
        );
        assertThat(failure.getMessage(), containsString("SEQUENCE is not supported by all nodes in the cluster"));
    }

    /** Named suffix limits survive retained-output preparation and normal optimizer reconstruction. */
    public void testSurfaceBoundsSurviveResolutionAndOptimization() {
        String query = """
            SEQUENCE WITHIN 1 minute PRIMARY later
            (FROM a) AS earlier_event BY key,
            (FROM b) AS later BY key WITHIN 5 seconds
            | EVAL earlier=MEMBER(earlier_event,key) | KEEP key,earlier | LIMIT 10
            """;
        // Labels must also be valid bare field identifiers when no MEMBER expression is present.
        String declaration = query.substring(0, query.indexOf("| EVAL"));
        for (String reserved : List.of("first", "last", "where")) {
            ParsingException rejected = expectThrows(
                ParsingException.class,
                () -> EsqlTestUtils.TEST_PARSER.createStatement(declaration.replace("AS earlier_event", "AS " + reserved) + "| KEEP key")
            );
            assertThat(rejected.getMessage(), containsString("stage name [" + reserved + "] must be an unquoted field identifier"));
        }
        LogicalPlan analyzed = configured(TransportVersion.current()).statement(query);
        assertEquals(
            analyzed.output().stream().map(Attribute::name).toList(),
            configured(TransportVersion.current()).statement(query.replace("earlier_event", "order"))
                .output()
                .stream()
                .map(Attribute::name)
                .toList()
        );
        Sequence sequence = analyzed.collect(Sequence.class).getFirst();
        assertEquals(60_000_000_000L, sequence.maxSpanNanos());
        assertEquals(0L, ((SequenceStage) sequence.children().getFirst()).maxGapNanos());
        assertEquals(5_000_000_000L, ((SequenceStage) sequence.children().get(1)).maxGapNanos());
        var optimizer = new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        );
        LogicalPlan optimized = optimizer.optimize(analyzed);
        assertEquals(analyzed.output(), optimized.output());
        assertEquals(5_000_000_000L, ((SequenceStage) optimized.collect(Sequence.class).getFirst().children().get(1)).maxGapNanos());
        configured(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5)).statementError(
            query,
            containsString("SEQUENCE is not supported by all nodes in the cluster")
        );
    }

    /** A redundant incoming bound remains accepted and cannot widen the independent overall window. */
    public void testIncomingLimitAtLeastTheWindowRemainsAccepted() {
        for (long incomingSeconds : List.of(60L, 120L)) {
            String query = """
                SEQUENCE BY key WITHIN 1 minute PRIMARY execution
                (FROM a) AS download, (FROM b) AS execution WITHIN %s seconds
                | KEEP key | LIMIT 10
                """.formatted(incomingSeconds);
            LogicalPlan analyzed = configured(TransportVersion.current()).statement(query);
            var optimizer = new LogicalPlanOptimizer(
                EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
            );
            LogicalPlan optimized = optimizer.optimize(analyzed);
            Sequence sequence = optimized.collect(Sequence.class).getFirst();
            assertEquals(60_000_000_000L, sequence.maxSpanNanos());
            assertEquals(incomingSeconds * 1_000_000_000L, ((SequenceStage) sequence.children().get(1)).maxGapNanos());
            assertEquals(List.of("key"), optimized.output().stream().map(Attribute::name).toList());
        }
    }

    private static LogicalPlan withLimit() {
        return EsqlTestUtils.TEST_PARSER.createStatement(QUERY).plan().transformUp(Sequence.class, sequence -> {
            var stages = new ArrayList<>(sequence.children());
            SequenceStage second = (SequenceStage) stages.get(1);
            stages.set(1, new SequenceStage(second.source(), second.child(), second.keys(), 5));
            return sequence.replaceChildren(stages);
        });
    }

    private static TestAnalyzer configured(TransportVersion minimumVersion) {
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "key",
            new EsField("key", DataType.KEYWORD, Map.of(), true, NONE)
        );
        TestAnalyzer analyzer = EsqlTestUtils.analyzer().minimumTransportVersion(minimumVersion);
        for (String name : List.of("a", "b")) {
            analyzer.addIndex(new EsIndex(name, mapping, Map.of(name, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
        }
        return analyzer;
    }
}
