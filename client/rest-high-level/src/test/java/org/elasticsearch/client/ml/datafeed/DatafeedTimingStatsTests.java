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
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DatafeedTimingStatsTests extends AbstractXContentTestCase<DatafeedTimingStats> {

    private static final String JOB_ID = "my-job-id";

    public static DatafeedTimingStats createRandomInstance() {
        return new DatafeedTimingStats(randomAlphaOfLength(10), randomLong(), randomDouble());
    }

    @Override
    protected DatafeedTimingStats createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected DatafeedTimingStats doParseInstance(XContentParser parser) throws IOException {
        return DatafeedTimingStats.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testParse_OptionalFieldsAbsent() throws IOException {
        String json = "{\"job_id\": \"my-job-id\"}";
        try (XContentParser parser =
                 XContentFactory.xContent(XContentType.JSON).createParser(
                     xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            DatafeedTimingStats stats = DatafeedTimingStats.PARSER.apply(parser, null);
            assertThat(stats.getJobId(), equalTo(JOB_ID));
            assertThat(stats.getSearchCount(), equalTo(0L));
            assertThat(stats.getTotalSearchTimeMs(), equalTo(0.0));
        }
    }

    public void testEquals() {
        DatafeedTimingStats stats1 = new DatafeedTimingStats(JOB_ID, 5, 100.0);
        DatafeedTimingStats stats2 = new DatafeedTimingStats(JOB_ID, 5, 100.0);
        DatafeedTimingStats stats3 = new DatafeedTimingStats(JOB_ID, 5, 200.0);

        assertTrue(stats1.equals(stats1));
        assertTrue(stats1.equals(stats2));
        assertFalse(stats2.equals(stats3));
    }

    public void testHashCode() {
        DatafeedTimingStats stats1 = new DatafeedTimingStats(JOB_ID, 5, 100.0);
        DatafeedTimingStats stats2 = new DatafeedTimingStats(JOB_ID, 5, 100.0);
        DatafeedTimingStats stats3 = new DatafeedTimingStats(JOB_ID, 5, 200.0);

        assertEquals(stats1.hashCode(), stats1.hashCode());
        assertEquals(stats1.hashCode(), stats2.hashCode());
        assertNotEquals(stats2.hashCode(), stats3.hashCode());
    }

    public void testConstructorAndGetters() {
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID, 5, 123.456);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(5L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(123.456));
    }
}
