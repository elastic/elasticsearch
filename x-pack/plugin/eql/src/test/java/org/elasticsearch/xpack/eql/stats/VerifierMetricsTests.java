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

import static org.elasticsearch.xpack.eql.stats.FeatureMetric.EVENT;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_ONE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_THREE;
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
import static org.elasticsearch.xpack.eql.stats.Metrics.FPREFIX;
import static org.elasticsearch.xpack.eql.stats.Metrics.JOIN_PREFIX;
import static org.elasticsearch.xpack.eql.stats.Metrics.KEYS_PREFIX;
import static org.elasticsearch.xpack.eql.stats.Metrics.PIPES_PREFIX;
import static org.elasticsearch.xpack.eql.stats.Metrics.SEQUENCE_PREFIX;

public class VerifierMetricsTests extends ESTestCase {
    
    private EqlParser parser = new EqlParser();
    private PreAnalyzer preAnalyzer = new PreAnalyzer();
    private EqlFunctionRegistry eqlFunctionRegistry = new EqlFunctionRegistry();
    private IndexResolution index = OptimizerTests.loadIndexResolution("mapping-default.json");
    protected static String FEATURES_KEYS_PREFIX = FPREFIX + KEYS_PREFIX;
    protected static String FEATURES_JOIN_PREFIX = FPREFIX + JOIN_PREFIX;
    protected static String FEATURES_SEQUENCE_PREFIX = FPREFIX + SEQUENCE_PREFIX;
    protected static String FEATURES_PIPES_PREFIX = FPREFIX + PIPES_PREFIX;
    
    public void testEventQuery() {
        Counters c = eql("process where serial_event_id < 4");
        assertCounters(0, 1L, 0, 1L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, c);
    }
    
    public void testSequenceQuery() {
        Counters c = eql("sequence\r\n" + 
            "  [process where serial_event_id = 1]\r\n" + 
            "  [process where serial_event_id = 2]");
        assertCounters(1L, 0, 0, 1L, 0, 0, 0, 0, 1L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, c);
    }

    @AwaitsFix(bugUrl = "waiting for the join implementation")
    public void testJoinQuery() {
        Counters c = eql("join\r\n" + 
            "  [file where file_name=\"*.exe\"] by ppid\r\n" + 
            "  [file where file_name=\"*.com\"] by pid\r\n" + 
            "until [process where opcode=1] by ppid\r\n" + 
            "| head 1");
        assertCounters(0, 0, 1L, 1L, 0, 0, 0, 1L, 1L, 0, 0, 0, 0, 0, 0, 0, 1L, 0, 0, 0, 0, c);
    }
    
    public void testHeadQuery() {
        Counters c = eql("process where serial_event_id < 4 | head 2");
        assertCounters(0, 1L, 0, 1L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, c);
    }
    
    public void testTailQuery() {
        Counters c = eql("process where serial_event_id < 4 | tail 2");
        assertCounters(0, 1L, 0, 0, 1L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, c);
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
        assertCounters(1L, 0, 0, 1L, 1L, 1L, 1L, 0, 0, 0, 1L, 0, 0, 0, 0, 0, 1L, 0, 0, 0, 0, c);
    }
    
    public void testSequenceWithTwoQueries() {
        Counters c = eql("sequence with maxspan=1d\r\n" + 
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "until [process where opcode=1] by ppid\r\n" + 
            "| head 4\r\n" + 
            "| tail 2");
        assertCounters(1L, 0, 0, 1L, 1L, 1L, 1L, 0, 1L, 0, 0, 0, 0, 0, 0, 0, 1L, 0, 0, 0, 0, c);
    }
    
    public void testSequenceWithThreeQueries() {
        Counters c = eql("sequence with maxspan=1d\r\n" + 
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "  [file where parent_process_name == \"file_delete_event\"] by exit_code\r\n" + 
            "| head 4");
        assertCounters(1L, 0, 0, 1L, 0, 1L, 0, 0, 0, 1L, 0, 0, 0, 0, 0, 0, 1L, 0, 0, 0, 0, c);
    }
    
    public void testSequenceWithFiveQueries() {
        Counters c = eql("sequence with maxspan=1d\r\n" + 
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "  [file where parent_process_name == \"file_delete_event\"] by exit_code\r\n" +
            "  [process where serial_event_id < 4] by exit_code\r\n" + 
            "  [process where opcode == 1] by user\r\n" + 
            "| head 4");
        assertCounters(1L, 0, 0, 1L, 0, 1L, 0, 0, 0, 0, 0, 1L, 0, 0, 0, 0, 1L, 0, 0, 0, 0, c);
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
        assertCounters(1L, 0, 0, 0, 1L, 0, 0, 0, 0, 0, 0, 1L, 0, 0, 0, 0, 0, 1L, 0, 0, 0, c);
    }
    
    public void testSequenceWithThreeKeys() {
        Counters c = eql("sequence by exit_code, user, serial_event_id\r\n" + 
            "  [process where serial_event_id < 4]\r\n" + 
            "  [process where opcode == 1]\r\n");
        assertCounters(1L, 0, 0, 1L, 0, 0, 0, 0, 1L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1L, 0, 0, c);
    }

    public void testSequenceWithFourKeys() {
        Counters c = eql("sequence by exit_code, user, serial_event_id, pid\r\n" + 
            "  [process where serial_event_id < 4]\r\n" + 
            "  [process where opcode == 1]\r\n");
        assertCounters(1L, 0, 0, 1L, 0, 0, 0, 0, 1L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1L, 0, c);
    }

    public void testSequenceWithFiveKeys() {
        Counters c = eql("sequence by exit_code, user, serial_event_id, pid, ppid\r\n" + 
            "  [process where serial_event_id < 4]\r\n" + 
            "  [process where opcode == 1]\r\n");
        assertCounters(1L, 0, 0, 1L, 0, 0, 0, 0, 1L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1L, c);
    }    

    private void assertCounters(long sequence, long event, long join, long head, long tail, long seqMaxSpan, long seqUntil, long joinUntil,
        long seqQTwo, long seqQThree, long seqQFour, long seqQFive, long joinQTwo, long joinQThree, long joinQFour, long joingQFive,
        long keysOne, long keysTwo, long keysThree, long keysFour, long keysFive, Counters c)
    {
        assertEquals(sequence, c.get(FPREFIX + SEQUENCE));
        assertEquals(event, c.get(FPREFIX + EVENT));
        assertEquals(join, c.get(FPREFIX + JOIN));
        assertEquals(head, c.get(FEATURES_PIPES_PREFIX + PIPE_HEAD));
        assertEquals(tail, c.get(FEATURES_PIPES_PREFIX + PIPE_TAIL));
        assertEquals(seqMaxSpan, c.get(FEATURES_SEQUENCE_PREFIX + SEQUENCE_MAXSPAN));
        assertEquals(seqUntil, c.get(FEATURES_SEQUENCE_PREFIX + SEQUENCE_UNTIL));
        assertEquals(joinUntil, c.get(FEATURES_JOIN_PREFIX + JOIN_UNTIL));
        assertEquals(seqQTwo, c.get(FEATURES_SEQUENCE_PREFIX + SEQUENCE_QUERIES_TWO));
        assertEquals(seqQThree, c.get(FEATURES_SEQUENCE_PREFIX + SEQUENCE_QUERIES_THREE));
        assertEquals(seqQFour, c.get(FEATURES_SEQUENCE_PREFIX + SEQUENCE_QUERIES_FOUR));
        assertEquals(seqQFive, c.get(FEATURES_SEQUENCE_PREFIX + SEQUENCE_QUERIES_FIVE_OR_MORE));
        assertEquals(joinQTwo, c.get(FEATURES_JOIN_PREFIX + JOIN_QUERIES_TWO));
        assertEquals(joinQThree, c.get(FEATURES_JOIN_PREFIX + JOIN_QUERIES_THREE));
        assertEquals(joinQFour, c.get(FEATURES_JOIN_PREFIX + JOIN_QUERIES_FOUR));
        assertEquals(joingQFive, c.get(FEATURES_JOIN_PREFIX + JOIN_QUERIES_FIVE_OR_MORE));
        assertEquals(keysOne, c.get(FEATURES_KEYS_PREFIX + JOIN_KEYS_ONE));
        assertEquals(keysTwo, c.get(FEATURES_KEYS_PREFIX + JOIN_KEYS_TWO));
        assertEquals(keysThree, c.get(FEATURES_KEYS_PREFIX + JOIN_KEYS_THREE));
        assertEquals(keysFour, c.get(FEATURES_KEYS_PREFIX + JOIN_KEYS_FOUR));
        assertEquals(keysFive, c.get(FEATURES_KEYS_PREFIX + JOIN_KEYS_FIVE_OR_MORE));
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
}