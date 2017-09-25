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

package org.elasticsearch.index.reindex;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.Math.abs;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.hamcrest.Matchers.hasSize;

public class BulkByScrollTaskStatusTests extends ESTestCase {
    public void testBulkByTaskStatus() throws IOException {
        BulkByScrollTask.Status status = randomStatus();
        BytesStreamOutput out = new BytesStreamOutput();
        status.writeTo(out);
        BulkByScrollTask.Status tripped = new BulkByScrollTask.Status(out.bytes().streamInput());
        assertTaskStatusEquals(out.getVersion(), status, tripped);

        // Also check round tripping pre-5.1 which is the first version to support parallelized scroll
        out = new BytesStreamOutput();
        out.setVersion(Version.V_5_0_0_rc1); // This can be V_5_0_0
        status.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_0_0_rc1);
        tripped = new BulkByScrollTask.Status(in);
        assertTaskStatusEquals(Version.V_5_0_0_rc1, status, tripped);
    }

    /**
     * Assert that two task statuses are equal after serialization.
     * @param version the version at which expected was serialized
     */
    public static void assertTaskStatusEquals(Version version, BulkByScrollTask.Status expected, BulkByScrollTask.Status actual) {
        assertEquals(expected.getTotal(), actual.getTotal());
        assertEquals(expected.getUpdated(), actual.getUpdated());
        assertEquals(expected.getCreated(), actual.getCreated());
        assertEquals(expected.getDeleted(), actual.getDeleted());
        assertEquals(expected.getBatches(), actual.getBatches());
        assertEquals(expected.getVersionConflicts(), actual.getVersionConflicts());
        assertEquals(expected.getNoops(), actual.getNoops());
        assertEquals(expected.getBulkRetries(), actual.getBulkRetries());
        assertEquals(expected.getSearchRetries(), actual.getSearchRetries());
        assertEquals(expected.getThrottled(), actual.getThrottled());
        assertEquals(expected.getRequestsPerSecond(), actual.getRequestsPerSecond(), 0f);
        assertEquals(expected.getReasonCancelled(), actual.getReasonCancelled());
        assertEquals(expected.getThrottledUntil(), actual.getThrottledUntil());
        if (version.onOrAfter(Version.V_5_1_1)) {
            assertThat(actual.getSliceStatuses(), Matchers.hasSize(expected.getSliceStatuses().size()));
            for (int i = 0; i < expected.getSliceStatuses().size(); i++) {
                BulkByScrollTask.StatusOrException sliceStatus = expected.getSliceStatuses().get(i);
                if (sliceStatus == null) {
                    assertNull(actual.getSliceStatuses().get(i));
                } else if (sliceStatus.getException() == null) {
                    assertNull(actual.getSliceStatuses().get(i).getException());
                    assertTaskStatusEquals(version, sliceStatus.getStatus(), actual.getSliceStatuses().get(i).getStatus());
                } else {
                    assertNull(actual.getSliceStatuses().get(i).getStatus());
                    // Just check the message because we're not testing exception serialization in general here.
                    assertEquals(sliceStatus.getException().getMessage(), actual.getSliceStatuses().get(i).getException().getMessage());
                }
            }
        } else {
            assertEquals(emptyList(), actual.getSliceStatuses());
        }
    }

    public static BulkByScrollTask.Status randomStatus() {
        if (randomBoolean()) {
            return randomWorkingStatus(null);
        }
        boolean canHaveNullStatues = randomBoolean();
        List<BulkByScrollTask.StatusOrException> statuses = IntStream.range(0, between(0, 10))
                .mapToObj(i -> {
                    if (canHaveNullStatues && LuceneTestCase.rarely()) {
                        return null;
                    }
                    if (randomBoolean()) {
                        return new BulkByScrollTask.StatusOrException(new ElasticsearchException(randomAlphaOfLength(5)));
                    }
                    return new BulkByScrollTask.StatusOrException(randomWorkingStatus(i));
                })
                .collect(toList());
        return new BulkByScrollTask.Status(statuses, randomBoolean() ? "test" : null);
    }

    private static BulkByScrollTask.Status randomWorkingStatus(Integer sliceId) {
        // These all should be believably small because we sum them if we have multiple workers
        int total = between(0, 10000000);
        int updated = between(0, total);
        int created = between(0, total - updated);
        int deleted = between(0, total - updated - created);
        int noops = total - updated - created - deleted;
        int batches = between(0, 10000);
        long versionConflicts = between(0, total);
        long bulkRetries = between(0, 10000000);
        long searchRetries = between(0, 100000);
        return new BulkByScrollTask.Status(sliceId, total, updated, created, deleted, batches, versionConflicts, noops, bulkRetries,
                searchRetries, parseTimeValue(randomPositiveTimeValue(), "test"), abs(Randomness.get().nextFloat()),
                randomBoolean() ? null : randomSimpleString(Randomness.get()), parseTimeValue(randomPositiveTimeValue(), "test"));
    }
}
