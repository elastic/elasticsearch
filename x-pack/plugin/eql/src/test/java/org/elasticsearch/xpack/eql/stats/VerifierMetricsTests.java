/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

import java.util.Set;

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
        Counters c = eql("sequence\r\n" + 
            "  [process where serial_event_id = 1]\r\n" + 
            "  [process where serial_event_id = 2]");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_QUERIES_TWO));
    }

    @AwaitsFix(bugUrl = "waiting for the join implementation")
    public void testJoinQuery() {
        Counters c = eql("join\r\n" + 
            "  [file where file_name=\"*.exe\"] by ppid\r\n" + 
            "  [file where file_name=\"*.com\"] by pid\r\n" + 
            "until [process where opcode=1] by ppid\r\n" + 
            "| head 1");
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
        Counters c = eql("sequence with maxspan=1d\r\n" + 
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "  [process where opcode == 2] by user\r\n" + 
            "  [file where parent_process_name == \"file_delete_event\"] by exit_code\r\n" +
            "until [process where opcode=1] by ppid\r\n" + 
            "| head 4\r\n" + 
            "| tail 2");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, PIPE_TAIL, SEQUENCE_MAXSPAN, SEQUENCE_UNTIL, SEQUENCE_QUERIES_FOUR, JOIN_KEYS_ONE));
    }
    
    public void testSequenceWithTwoQueries() {
        Counters c = eql("sequence with maxspan=1d\r\n" + 
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "until [process where opcode=1] by ppid\r\n" + 
            "| head 4\r\n" + 
            "| tail 2");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, PIPE_TAIL, SEQUENCE_MAXSPAN, SEQUENCE_UNTIL, SEQUENCE_QUERIES_TWO, JOIN_KEYS_ONE));
    }
    
    public void testSequenceWithThreeQueries() {
        Counters c = eql("sequence with maxspan=1d\r\n" + 
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "  [file where parent_process_name == \"file_delete_event\"] by exit_code\r\n" + 
            "| head 4");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_MAXSPAN, SEQUENCE_QUERIES_THREE, JOIN_KEYS_ONE));
    }
    
    public void testSequenceWithFiveQueries() {
        Counters c = eql("sequence with maxspan=1d\r\n" + 
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "  [file where parent_process_name == \"file_delete_event\"] by exit_code\r\n" +
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "| head 4");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_MAXSPAN, SEQUENCE_QUERIES_FIVE_OR_MORE, JOIN_KEYS_ONE));
    }

    public void testSequenceWithSevenQueries() {
        Counters c = eql("sequence by exit_code, user\r\n" + 
            "  [process where serial_event_id < 4]\r\n" + 
            "  [process where opcode == 1]\r\n" + 
            "  [file where parent_process_name == \"file_delete_event\"]\r\n" +
            "  [process where serial_event_id < 4]\r\n" + 
            "  [process where opcode == 1]\r\n" + 
            "  [process where true]\r\n" + 
            "  [process where true]\r\n" + 
            "| tail 1");
        assertCounters(c, Set.of(SEQUENCE, PIPE_TAIL, SEQUENCE_QUERIES_FIVE_OR_MORE, JOIN_KEYS_TWO));
    }
    
    public void testSequenceWithThreeKeys() {
        Counters c = eql("sequence by exit_code, user, serial_event_id\r\n" + 
            "  [process where serial_event_id < 4]\r\n" + 
            "  [process where opcode == 1]\r\n");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_QUERIES_TWO, JOIN_KEYS_THREE));
    }

    public void testSequenceWithFourKeys() {
        Counters c = eql("sequence by exit_code, user, serial_event_id, pid\r\n" + 
            "  [process where serial_event_id < 4]\r\n" + 
            "  [process where opcode == 1]\r\n");
        assertCounters(c, Set.of(SEQUENCE, PIPE_HEAD, SEQUENCE_QUERIES_TWO, JOIN_KEYS_FOUR));
    }

    public void testSequenceWithFiveKeys() {
        Counters c = eql("sequence by exit_code, user, serial_event_id, pid, ppid\r\n" + 
            "  [process where serial_event_id < 4]\r\n" + 
            "  [process where opcode == 1]\r\n");
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
        return eql(query, null);
    }

    private Counters eql(String query, Verifier v) {        
        Verifier verifier = v;
        Metrics metrics = null;
        if (v == null) {
            metrics = new Metrics();
            verifier = new Verifier(metrics);
        }
        Analyzer analyzer = new Analyzer(EqlTestUtils.randomConfiguration(), eqlFunctionRegistry, verifier);
        analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(query), index));
        
        return metrics == null ? null : metrics.stats();
    }

    private class MetricsHolder {
        long[] metrics;

        MetricsHolder() {
            this.metrics = new long[FeatureMetric.values().length];
            for (int i = 0; i < this.metrics.length; i++) {
                this.metrics[i] = 0;
            }
        }

        void set(Set<FeatureMetric> metrics) {
            for (FeatureMetric metric : metrics) {
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