/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.eql.EqlTestUtils;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.OptimizerTests;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.ql.index.IndexResolution;

import java.util.Arrays;
import java.util.Set;

import static org.elasticsearch.xpack.eql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.EVENT;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_ONE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_UNTIL;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.PIPE_HEAD;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.PIPE_TAIL;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_MAXSPAN;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_UNTIL;

public class VerifierMetricsTests extends ESTestCase {

    private EqlParser parser = new EqlParser();
    private PreAnalyzer preAnalyzer = new PreAnalyzer();
    private EqlFunctionRegistry eqlFunctionRegistry = new EqlFunctionRegistry();
    private IndexResolution index = OptimizerTests.loadIndexResolution("mapping-default.json");

    public void testEventQuery() {
        Counters c = eql("process where serial_event_id < 4");
        assertCounters(c, Set.of(EVENT, PIPE_HEAD));
    }

    public void testSequenceQuery() {
        Counters c = eql("""
            sequence\r
              [process where serial_event_id == 1]\r
              [process where serial_event_id == 2]""");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_QUERIES_TWO));
    }

    @AwaitsFix(bugUrl = "waiting for the join implementation")
    public void testJoinQuery() {
        Counters c = eql("""
            join\r
              [file where file_name="*.exe"] by ppid\r
              [file where file_name="*.com"] by pid\r
            until [process where opcode=1] by ppid\r
            | head 1""");
        assertCounters(c, Set.of(JOIN, PIPE_HEAD, JOIN_UNTIL, JOIN_QUERIES_TWO, JOIN_KEYS_ONE));
    }

    public void testHeadQuery() {
        Counters c = eql("process where serial_event_id < 4 | head 2");
        assertCounters(c, Set.of(EVENT, PIPE_HEAD));
    }

    public void testTailQuery() {
        Counters c = eql("process where serial_event_id < 4 | tail 2");
        assertCounters(c, Set.of(EVENT, PIPE_TAIL));
    }

    public void testSequenceMaxSpanQuery() {
        Counters c = eql("""
            sequence with maxspan=1d\r
              [process where serial_event_id < 4] by exit_code\r
              [process where opcode == 1] by opcode\r
              [process where opcode == 2] by opcode\r
              [file where parent_process_name == "file_delete_event"] by exit_code\r
            until [process where opcode==1] by ppid\r
            | head 4\r
            | tail 2""");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, PIPE_TAIL, SEQUENCE_MAXSPAN, SEQUENCE_UNTIL, SEQUENCE_QUERIES_FOUR, JOIN_KEYS_ONE));
    }

    public void testSequenceWithTwoQueries() {
        Counters c = eql("""
            sequence with maxspan=1d\r
              [process where serial_event_id < 4] by exit_code\r
              [process where opcode == 1] by opcode\r
            until [process where opcode==1] by ppid\r
            | head 4\r
            | tail 2""");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, PIPE_TAIL, SEQUENCE_MAXSPAN, SEQUENCE_UNTIL, SEQUENCE_QUERIES_TWO, JOIN_KEYS_ONE));
    }

    public void testSequenceWithThreeQueries() {
        Counters c = eql("""
            sequence with maxspan=1d\r
              [process where serial_event_id < 4] by exit_code\r
              [process where opcode == 1] by opcode\r
              [file where parent_process_name == "file_delete_event"] by exit_code\r
            | head 4""");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_MAXSPAN, SEQUENCE_QUERIES_THREE, JOIN_KEYS_ONE));
    }

    public void testSequenceWithFiveQueries() {
        Counters c = eql("""
            sequence with maxspan=1d\r
              [process where serial_event_id < 4] by exit_code\r
              [process where opcode == 1] by opcode\r
              [file where parent_process_name == "file_delete_event"] by exit_code\r
              [process where serial_event_id < 4] by exit_code\r
              [process where opcode == 1] by opcode\r
            | head 4""");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_MAXSPAN, SEQUENCE_QUERIES_FIVE_OR_MORE, JOIN_KEYS_ONE));
    }

    public void testSequenceWithSevenQueries() {
        Counters c = eql("""
            sequence by exit_code, opcode\r
              [process where serial_event_id < 4]\r
              [process where opcode == 1]\r
              [file where parent_process_name == "file_delete_event"]\r
              [process where serial_event_id < 4]\r
              [process where opcode == 1]\r
              [process where true]\r
              [process where true]\r
            | tail 1""");
        assertCounters(c, Set.of(SEQUENCE, PIPE_TAIL, SEQUENCE_QUERIES_FIVE_OR_MORE, JOIN_KEYS_TWO));
    }

    public void testSequenceWithThreeKeys() {
        Counters c = eql("""
            sequence by exit_code, opcode, serial_event_id\r
              [process where serial_event_id < 4]\r
              [process where opcode == 1]\r
            """);
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_QUERIES_TWO, JOIN_KEYS_THREE));
    }

    public void testSequenceWithFourKeys() {
        Counters c = eql("""
            sequence by exit_code, user, serial_event_id, pid\r
              [process where serial_event_id < 4]\r
              [process where opcode == 1]\r
            """);
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_QUERIES_TWO, JOIN_KEYS_FOUR));
    }

    public void testSequenceWithFiveKeys() {
        Counters c = eql("""
            sequence by exit_code, user, serial_event_id, pid, ppid\r
              [process where serial_event_id < 4]\r
              [process where opcode == 1]\r
            """);
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_QUERIES_TWO, JOIN_KEYS_FIVE_OR_MORE));
    }

    private void assertCounters(Counters actual, Set<FeatureMetric> metrics) {
        MetricsHolder expected = new MetricsHolder();
        expected.set(metrics);

        for (FeatureMetric metric : FeatureMetric.values()) {
            assertEquals(expected.get(metric), actual.get(metric.prefixedName()));
        }
    }

    private Counters eql(String query) {
        Metrics metrics = new Metrics();
        Verifier verifier = new Verifier(metrics);
        Analyzer analyzer = analyzer(EqlTestUtils.randomConfiguration(), eqlFunctionRegistry, verifier);
        analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(query), index));
        return metrics.stats();
    }

    private static class MetricsHolder {
        long[] metrics;

        MetricsHolder() {
            this.metrics = new long[FeatureMetric.values().length];
            Arrays.fill(this.metrics, 0);
        }

        void set(Set<FeatureMetric> metricSet) {
            for (FeatureMetric metric : metricSet) {
                set(metric);
            }
        }

        void set(FeatureMetric metric) {
            this.metrics[metric.ordinal()] = 1L;
        }

        long get(FeatureMetric metric) {
            return this.metrics[metric.ordinal()];
        }
    }
}
