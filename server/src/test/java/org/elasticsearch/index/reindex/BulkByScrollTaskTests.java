/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;

import static java.lang.Math.min;
import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.hamcrest.Matchers.containsString;

public class BulkByScrollTaskTests extends ESTestCase {
    public void testStatusHatesNegatives() {
        checkStatusNegatives(-1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "sliceId");
        checkStatusNegatives(null, -1, 0, 0, 0, 0, 0, 0, 0, 0, "total");
        checkStatusNegatives(null, 0, -1, 0, 0, 0, 0, 0, 0, 0, "updated");
        checkStatusNegatives(null, 0, 0, -1, 0, 0, 0, 0, 0, 0, "created");
        checkStatusNegatives(null, 0, 0, 0, -1, 0, 0, 0, 0, 0, "deleted");
        checkStatusNegatives(null, 0, 0, 0, 0, -1, 0, 0, 0, 0, "batches");
        checkStatusNegatives(null, 0, 0, 0, 0, 0, -1, 0, 0, 0, "versionConflicts");
        checkStatusNegatives(null, 0, 0, 0, 0, 0, 0, -1, 0, 0, "noops");
        checkStatusNegatives(null, 0, 0, 0, 0, 0, 0, 0, -1, 0, "bulkRetries");
        checkStatusNegatives(null, 0, 0, 0, 0, 0, 0, 0, 0, -1, "searchRetries");
    }

    /**
     * Build a task status with only some values. Used for testing negative values.
     */
    private void checkStatusNegatives(
        Integer sliceId,
        long total,
        long updated,
        long created,
        long deleted,
        int batches,
        long versionConflicts,
        long noops,
        long bulkRetries,
        long searchRetries,
        String fieldName
    ) {
        TimeValue throttle = parseTimeValue(randomPositiveTimeValue(), "test");
        TimeValue throttledUntil = parseTimeValue(randomPositiveTimeValue(), "test");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new BulkByScrollTask.Status(
                sliceId,
                total,
                updated,
                created,
                deleted,
                batches,
                versionConflicts,
                noops,
                bulkRetries,
                searchRetries,
                throttle,
                0f,
                null,
                throttledUntil
            )
        );
        assertEquals(e.getMessage(), fieldName + " must be greater than 0 but was [-1]");
    }

    public void testXContentRepresentationOfUnlimitedRequestsPerSecond() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            null,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            timeValueMillis(0),
            Float.POSITIVE_INFINITY,
            null,
            timeValueMillis(0)
        );
        status.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), containsString("\"requests_per_second\":-1"));
    }

    public void testXContentRepresentationOfUnfinishedSlices() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        BulkByScrollTask.Status completedStatus = new BulkByScrollTask.Status(
            2,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            timeValueMillis(0),
            Float.POSITIVE_INFINITY,
            null,
            timeValueMillis(0)
        );
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            Arrays.asList(null, null, new BulkByScrollTask.StatusOrException(completedStatus)),
            null
        );
        status.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), containsString("\"slices\":[null,null,{\"slice_id\":2"));
    }

    public void testXContentRepresentationOfSliceFailures() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        Exception e = new Exception();
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            Arrays.asList(null, null, new BulkByScrollTask.StatusOrException(e)),
            null
        );
        status.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), containsString("\"slices\":[null,null,{\"type\":\"exception\""));
    }

    public void testMergeStatuses() {
        BulkByScrollTask.StatusOrException[] statuses = new BulkByScrollTask.StatusOrException[between(2, 100)];
        boolean containsNullStatuses = randomBoolean();
        int mergedTotal = 0;
        int mergedUpdated = 0;
        int mergedCreated = 0;
        int mergedDeleted = 0;
        int mergedBatches = 0;
        int mergedVersionConflicts = 0;
        int mergedNoops = 0;
        int mergedBulkRetries = 0;
        int mergedSearchRetries = 0;
        TimeValue mergedThrottled = timeValueNanos(0);
        float mergedRequestsPerSecond = 0;
        TimeValue mergedThrottledUntil = timeValueNanos(Integer.MAX_VALUE);
        for (int i = 0; i < statuses.length; i++) {
            if (containsNullStatuses && rarely()) {
                continue;
            }
            int total = between(0, 10000);
            int updated = between(0, total);
            int created = between(0, total - updated);
            int deleted = between(0, total - updated - created);
            int batches = between(0, 10);
            int versionConflicts = between(0, 100);
            int noops = total - updated - created - deleted;
            int bulkRetries = between(0, 100);
            int searchRetries = between(0, 100);
            TimeValue throttled = timeValueNanos(between(0, 10000));
            float requestsPerSecond = randomValueOtherThanMany(r -> r <= 0, () -> randomFloat());
            String reasonCancelled = randomBoolean() ? null : "test";
            TimeValue throttledUntil = timeValueNanos(between(0, 1000));
            statuses[i] = new BulkByScrollTask.StatusOrException(
                new BulkByScrollTask.Status(
                    i,
                    total,
                    updated,
                    created,
                    deleted,
                    batches,
                    versionConflicts,
                    noops,
                    bulkRetries,
                    searchRetries,
                    throttled,
                    requestsPerSecond,
                    reasonCancelled,
                    throttledUntil
                )
            );
            mergedTotal += total;
            mergedUpdated += updated;
            mergedCreated += created;
            mergedDeleted += deleted;
            mergedBatches += batches;
            mergedVersionConflicts += versionConflicts;
            mergedNoops += noops;
            mergedBulkRetries += bulkRetries;
            mergedSearchRetries += searchRetries;
            mergedThrottled = timeValueNanos(mergedThrottled.nanos() + throttled.nanos());
            mergedRequestsPerSecond += requestsPerSecond;
            mergedThrottledUntil = timeValueNanos(min(mergedThrottledUntil.nanos(), throttledUntil.nanos()));
        }
        String reasonCancelled = randomBoolean() ? randomAlphaOfLength(10) : null;
        BulkByScrollTask.Status merged = new BulkByScrollTask.Status(Arrays.asList(statuses), reasonCancelled);
        assertEquals(mergedTotal, merged.getTotal());
        assertEquals(mergedUpdated, merged.getUpdated());
        assertEquals(mergedCreated, merged.getCreated());
        assertEquals(mergedDeleted, merged.getDeleted());
        assertEquals(mergedBatches, merged.getBatches());
        assertEquals(mergedVersionConflicts, merged.getVersionConflicts());
        assertEquals(mergedNoops, merged.getNoops());
        assertEquals(mergedBulkRetries, merged.getBulkRetries());
        assertEquals(mergedSearchRetries, merged.getSearchRetries());
        assertEquals(mergedThrottled, merged.getThrottled());
        assertEquals(mergedRequestsPerSecond, merged.getRequestsPerSecond(), 0.0001f);
        assertEquals(mergedThrottledUntil, merged.getThrottledUntil());
        assertEquals(reasonCancelled, merged.getReasonCancelled());
    }

}
