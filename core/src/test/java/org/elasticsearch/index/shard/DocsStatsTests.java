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

package org.elasticsearch.index.shard;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DocsStatsTests extends ESTestCase {

    public void testCalculateAverageDocSize() throws Exception {
        DocsStats stats = new DocsStats(10, 2, 10);
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));

        stats.add(new DocsStats(0, 0, randomNonNegativeLong()));
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));

        // (38*900 + 12*10) / 50 = 686L
        stats.add(new DocsStats(8, 30, 900));
        assertThat(stats.getCount(), equalTo(18L));
        assertThat(stats.getDeleted(), equalTo(32L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(686L));

        // (50*686 + 40*120) / 90 = 434L
        stats.add(new DocsStats(0, 40, 120));
        assertThat(stats.getCount(), equalTo(18L));
        assertThat(stats.getDeleted(), equalTo(72L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(434L));

        // (90*434 + 35*99) / 125 = 340L
        stats.add(new DocsStats(35, 0, 99));
        assertThat(stats.getCount(), equalTo(53L));
        assertThat(stats.getDeleted(), equalTo(72L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(340L));
    }

    public void testSerialize() throws Exception {
        DocsStats originalStats = new DocsStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStats.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = bytes.streamInput()) {
                DocsStats cloneStats = new DocsStats();
                cloneStats.readFrom(in);
                assertThat(cloneStats.getCount(), equalTo(originalStats.getCount()));
                assertThat(cloneStats.getDeleted(), equalTo(originalStats.getDeleted()));
                assertThat(cloneStats.getAverageSizeInBytes(), equalTo(originalStats.getAverageSizeInBytes()));
            }
        }
    }
}
