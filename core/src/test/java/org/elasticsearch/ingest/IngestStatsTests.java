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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class IngestStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        IngestStats.Stats total = new IngestStats.Stats(5, 10, 20, 30);
        IngestStats.Stats foo = new IngestStats.Stats(50, 100, 200, 300);
        IngestStats ingestStats = new IngestStats(total, Collections.singletonMap("foo", foo));
        IngestStats serialize = serialize(ingestStats);
        assertNotSame(serialize, ingestStats);
        assertNotSame(serialize.getTotalStats(), total);
        assertEquals(total.getIngestCount(), serialize.getTotalStats().getIngestCount());
        assertEquals(total.getIngestFailedCount(), serialize.getTotalStats().getIngestFailedCount());
        assertEquals(total.getIngestTimeInMillis(), serialize.getTotalStats().getIngestTimeInMillis());
        assertEquals(total.getIngestCurrent(), serialize.getTotalStats().getIngestCurrent());

        assertEquals(ingestStats.getStatsPerPipeline().size(), 1);
        assertTrue(ingestStats.getStatsPerPipeline().containsKey("foo"));

        Map<String, IngestStats.Stats> left = ingestStats.getStatsPerPipeline();
        Map<String, IngestStats.Stats> right = serialize.getStatsPerPipeline();

        assertEquals(right.size(), 1);
        assertTrue(right.containsKey("foo"));
        assertEquals(left.size(), 1);
        assertTrue(left.containsKey("foo"));
        IngestStats.Stats leftStats = left.get("foo");
        IngestStats.Stats rightStats = right.get("foo");
        assertEquals(leftStats.getIngestCount(), rightStats.getIngestCount());
        assertEquals(leftStats.getIngestFailedCount(), rightStats.getIngestFailedCount());
        assertEquals(leftStats.getIngestTimeInMillis(), rightStats.getIngestTimeInMillis());
        assertEquals(leftStats.getIngestCurrent(), rightStats.getIngestCurrent());
    }

    private IngestStats serialize(IngestStats stats) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes());
        return new IngestStats(in);
    }
}
