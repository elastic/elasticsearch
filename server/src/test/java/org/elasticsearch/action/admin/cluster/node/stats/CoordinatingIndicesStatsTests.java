package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kyra.wkh
 **/
public class CoordinatingIndicesStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        String index = "test";
        CoordinatingIndiceStats coordinatingIndiceStats = new CoordinatingIndiceStats(index, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        CounterMetric counterMetric1 = new CounterMetric();
        MeanMetric meanMetric1 = new MeanMetric();
        counterMetric1.inc(randomIntBetween(0, 10));
        int count = randomIntBetween(0, 10);
        for(int i=0; i<count; i++) {
            meanMetric1.inc(randomIntBetween(0, 10));
        }

        coordinatingIndiceStats.counterMetricMap.put("count1", counterMetric1);
        coordinatingIndiceStats.meanMatricMap.put("mean1", meanMetric1);

        CounterMetric counterMetric2 = new CounterMetric();
        MeanMetric meanMetric2 = new MeanMetric();
        counterMetric2.inc(randomIntBetween(0, 10));
        int count2 = randomIntBetween(0, 10);
        for(int i=0; i<count2; i++) {
            meanMetric2.inc(randomIntBetween(0, 10));
        }
        coordinatingIndiceStats.counterMetricMap.put("count2", counterMetric2);
        coordinatingIndiceStats.meanMatricMap.put("mean2", meanMetric2);


        try (BytesStreamOutput out = new BytesStreamOutput()) {
            coordinatingIndiceStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                CoordinatingIndiceStats deserializedStats = new CoordinatingIndiceStats(in);
                assertEquals(index, deserializedStats.indexName);
                assertEquals(2, deserializedStats.counterMetricMap.size());
                assertNotNull(deserializedStats.counterMetricMap.get("count1"));
                assertNotNull(deserializedStats.counterMetricMap.get("count2"));
                assertEquals(coordinatingIndiceStats.counterMetricMap.get("count1").count(), deserializedStats.counterMetricMap.get("count1").count());
                assertEquals(coordinatingIndiceStats.counterMetricMap.get("count2").count(), deserializedStats.counterMetricMap.get("count2").count());
                assertEquals(2, deserializedStats.meanMatricMap.size());
                assertNotNull(deserializedStats.meanMatricMap.get("mean1"));
                assertNotNull(deserializedStats.meanMatricMap.get("mean2"));
                assertEquals(coordinatingIndiceStats.meanMatricMap.get("mean1").sum(), deserializedStats.meanMatricMap.get("mean1").sum());
                assertEquals(coordinatingIndiceStats.meanMatricMap.get("mean1").count(), deserializedStats.meanMatricMap.get("mean1").count());
                assertEquals(coordinatingIndiceStats.meanMatricMap.get("mean2").sum(), deserializedStats.meanMatricMap.get("mean2").sum());
                assertEquals(coordinatingIndiceStats.meanMatricMap.get("mean2").count(), deserializedStats.meanMatricMap.get("mean2").count());
            }
        }
    }
}
