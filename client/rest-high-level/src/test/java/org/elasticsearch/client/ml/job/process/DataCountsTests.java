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
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.joda.time.DateTime;

import java.util.Date;

import static org.hamcrest.Matchers.greaterThan;

public class DataCountsTests extends AbstractXContentTestCase<DataCounts> {

    public static DataCounts createTestInstance(String jobId) {
        return new DataCounts(jobId, randomIntBetween(1, 1_000_000),
                randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                new DateTime(randomDateTimeZone()).toDate(), new DateTime(randomDateTimeZone()).toDate(),
                new DateTime(randomDateTimeZone()).toDate(), new DateTime(randomDateTimeZone()).toDate(),
                new DateTime(randomDateTimeZone()).toDate());
    }

    @Override
    public DataCounts createTestInstance() {
        return createTestInstance(randomAlphaOfLength(10));
    }

    @Override
    protected DataCounts doParseInstance(XContentParser parser) {
        return DataCounts.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testCountsEquals_GivenEqualCounts() {
        DataCounts counts1 = createCounts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        DataCounts counts2 = createCounts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

        assertTrue(counts1.equals(counts2));
        assertTrue(counts2.equals(counts1));
    }

    public void testCountsHashCode_GivenEqualCounts() {
        DataCounts counts1 = createCounts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        DataCounts counts2 = createCounts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        assertEquals(counts1.hashCode(), counts2.hashCode());
    }

    public void testCountsCopyConstructor() {
        DataCounts counts1 = createCounts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        DataCounts counts2 = new DataCounts(counts1);

        assertEquals(counts1.hashCode(), counts2.hashCode());
    }

    public void testCountCreatedZero() throws Exception {
        DataCounts counts = new DataCounts(randomAlphaOfLength(16));
        assertAllFieldsEqualZero(counts);
    }

    public void testCountCopyCreatedFieldsNotZero() throws Exception {
        DataCounts counts1 = createCounts(1, 200, 400, 3, 4, 5, 6, 7, 8, 9, 1479211200000L, 1479384000000L, 13, 14, 15);
        assertAllFieldsGreaterThanZero(counts1);

        DataCounts counts2 = new DataCounts(counts1);
        assertAllFieldsGreaterThanZero(counts2);
    }

    private void assertAllFieldsEqualZero(DataCounts stats) throws Exception {
        assertEquals(0L, stats.getProcessedRecordCount());
        assertEquals(0L, stats.getProcessedFieldCount());
        assertEquals(0L, stats.getInputBytes());
        assertEquals(0L, stats.getInputFieldCount());
        assertEquals(0L, stats.getInputRecordCount());
        assertEquals(0L, stats.getInvalidDateCount());
        assertEquals(0L, stats.getMissingFieldCount());
        assertEquals(0L, stats.getOutOfOrderTimeStampCount());
    }

    private void assertAllFieldsGreaterThanZero(DataCounts stats) throws Exception {
        assertThat(stats.getProcessedRecordCount(), greaterThan(0L));
        assertThat(stats.getProcessedFieldCount(), greaterThan(0L));
        assertThat(stats.getInputBytes(), greaterThan(0L));
        assertThat(stats.getInputFieldCount(), greaterThan(0L));
        assertThat(stats.getInputRecordCount(), greaterThan(0L));
        assertThat(stats.getInputRecordCount(), greaterThan(0L));
        assertThat(stats.getInvalidDateCount(), greaterThan(0L));
        assertThat(stats.getMissingFieldCount(), greaterThan(0L));
        assertThat(stats.getOutOfOrderTimeStampCount(), greaterThan(0L));
        assertThat(stats.getLatestRecordTimeStamp().getTime(), greaterThan(0L));
    }

    private static DataCounts createCounts(
            long processedRecordCount, long processedFieldCount, long inputBytes, long inputFieldCount,
            long invalidDateCount, long missingFieldCount, long outOfOrderTimeStampCount,
            long emptyBucketCount, long sparseBucketCount, long bucketCount,
            long earliestRecordTime, long latestRecordTime, long lastDataTimeStamp, long latestEmptyBucketTimeStamp,
            long latestSparseBucketTimeStamp) {

        DataCounts counts = new DataCounts("foo", processedRecordCount, processedFieldCount, inputBytes,
                inputFieldCount, invalidDateCount, missingFieldCount, outOfOrderTimeStampCount,
                emptyBucketCount, sparseBucketCount, bucketCount,
                new Date(earliestRecordTime), new Date(latestRecordTime),
                new Date(lastDataTimeStamp), new Date(latestEmptyBucketTimeStamp), new Date(latestSparseBucketTimeStamp));

        return counts;
    }

}
