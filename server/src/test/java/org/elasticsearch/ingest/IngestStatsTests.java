/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;


public class IngestStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        IngestStats.Stats totalStats = new IngestStats.Stats(50, 100, 200, 300);
        IngestStats.Stats pipeline1Stats = new IngestStats.Stats(3, 3, 3, 3);
        IngestStats.Stats processor1Stats = new IngestStats.Stats(1, 1, 1, 1);
        IngestStats.Stats processor2Stats = new IngestStats.Stats(2, 2, 2, 2);
        IngestStats.Stats pipeline2Stats = new IngestStats.Stats(47, 97, 197, 297);
        IngestStats.Stats processor3Stats = new IngestStats.Stats(47, 97, 197, 297);

        Map<String, Tuple<IngestStats.Stats, List<Tuple<String, IngestStats.Stats>>>> pipelinesStats = new HashMap<>(2);
        List<Tuple<String, IngestStats.Stats>> processorStats = new ArrayList<>(2);
        //pipeline1 -> processor1,processor2
        processorStats.add(new Tuple<>("processor1", processor1Stats));
        processorStats.add(new Tuple<>("processor2", processor2Stats));
        pipelinesStats.put("pipeline1", new Tuple<>(pipeline1Stats, processorStats));
        //pipeline2 -> processor3
        processorStats.clear();
        processorStats.add(new Tuple<>("processor3", processor3Stats));
        pipelinesStats.put("pipeline2", new Tuple<>(pipeline2Stats, processorStats));

        IngestStats ingestStats = new IngestStats(totalStats, pipelinesStats);
        IngestStats serialize = serialize(ingestStats);
        assertNotSame(serialize, ingestStats);
        assertNotSame(serialize.getTotalStats(), totalStats);
        assertEquals(totalStats.getIngestCount(), serialize.getTotalStats().getIngestCount());
        assertEquals(totalStats.getIngestFailedCount(), serialize.getTotalStats().getIngestFailedCount());
        assertEquals(totalStats.getIngestTimeInMillis(), serialize.getTotalStats().getIngestTimeInMillis());
        assertEquals(totalStats.getIngestCurrent(), serialize.getTotalStats().getIngestCurrent());

        assertEquals(ingestStats.getStatsPerPipeline().size(), 2);
        assertTrue(ingestStats.getStatsPerPipeline().containsKey("pipeline1"));
        assertTrue(ingestStats.getStatsPerPipeline().containsKey("pipeline2"));
        assertStats(ingestStats.getStatsForPipeline("pipeline1"), serialize.getStatsForPipeline("pipeline1"));
        assertStats(ingestStats.getStatsForPipeline("pipeline2"), serialize.getStatsForPipeline("pipeline2"));

        Iterator<Tuple<String, IngestStats.Stats>> it = serialize.getProcessorStatsForPipeline("pipeline1").iterator();
        for(Tuple<String, IngestStats.Stats> objectTuple : ingestStats.getProcessorStatsForPipeline("pipeline1")){
            Tuple<String, IngestStats.Stats> streamTuple = it.next();
            assertThat(objectTuple.v1(), equalTo(streamTuple.v1()));
            assertStats(objectTuple.v2(), streamTuple.v2());
        }

        it = serialize.getProcessorStatsForPipeline("pipeline2").iterator();
        for(Tuple<String, IngestStats.Stats> objectTuple : ingestStats.getProcessorStatsForPipeline("pipeline2")){
            Tuple<String, IngestStats.Stats> streamTuple = it.next();
            assertThat(objectTuple.v1(), equalTo(streamTuple.v1()));
            assertStats(objectTuple.v2(), streamTuple.v2());
        }
    }

    private void assertStats(IngestStats.Stats fromObject, IngestStats.Stats fromStream) {
        assertEquals(fromObject.getIngestCount(), fromStream.getIngestCount());
        assertEquals(fromObject.getIngestFailedCount(), fromStream.getIngestFailedCount());
        assertEquals(fromObject.getIngestTimeInMillis(), fromStream.getIngestTimeInMillis());
        assertEquals(fromObject.getIngestCurrent(), fromStream.getIngestCurrent());
    }

    private IngestStats serialize(IngestStats stats) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new IngestStats(in);
    }
}
