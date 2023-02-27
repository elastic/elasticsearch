/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask.Status;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.lang.Math.abs;
import static java.util.stream.Collectors.toList;
import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.hamcrest.Matchers.equalTo;

public class BulkByScrollTaskStatusTests extends AbstractXContentTestCase<BulkByScrollTask.Status> {

    private boolean includeUpdated;
    private boolean includeCreated;

    public void testBulkByTaskStatus() throws IOException {
        BulkByScrollTask.Status status = randomStatus();
        BytesStreamOutput out = new BytesStreamOutput();
        status.writeTo(out);
        BulkByScrollTask.Status tripped = new BulkByScrollTask.Status(out.bytes().streamInput());
        assertTaskStatusEquals(out.getTransportVersion(), status, tripped);
    }

    /**
     * Assert that two task statuses are equal after serialization.
     * @param version the version at which expected was serialized
     */
    public static void assertTaskStatusEquals(TransportVersion version, BulkByScrollTask.Status expected, BulkByScrollTask.Status actual) {
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
    }

    public static BulkByScrollTask.Status randomStatus() {
        if (randomBoolean()) {
            return randomWorkingStatus(null);
        }
        boolean canHaveNullStatues = randomBoolean();
        List<BulkByScrollTask.StatusOrException> statuses = IntStream.range(0, between(0, 10)).mapToObj(i -> {
            if (canHaveNullStatues && LuceneTestCase.rarely()) {
                return null;
            }
            if (randomBoolean()) {
                return new BulkByScrollTask.StatusOrException(new ElasticsearchException(randomAlphaOfLength(5)));
            }
            return new BulkByScrollTask.StatusOrException(randomWorkingStatus(i));
        }).collect(toList());
        return new BulkByScrollTask.Status(statuses, randomBoolean() ? "test" : null);
    }

    public static BulkByScrollTask.Status randomStatusWithoutException() {
        if (randomBoolean()) {
            return randomWorkingStatus(null);
        }
        boolean canHaveNullStatues = randomBoolean();
        List<BulkByScrollTask.StatusOrException> statuses = IntStream.range(0, between(0, 10)).mapToObj(i -> {
            if (canHaveNullStatues && LuceneTestCase.rarely()) {
                return null;
            }
            return new BulkByScrollTask.StatusOrException(randomWorkingStatus(i));
        }).collect(toList());
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
        // smallest unit of time during toXContent is Milliseconds
        TimeUnit[] timeUnits = { TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS };
        TimeValue throttled = new TimeValue(randomIntBetween(0, 1000), randomFrom(timeUnits));
        TimeValue throttledUntil = new TimeValue(randomIntBetween(0, 1000), randomFrom(timeUnits));
        return new BulkByScrollTask.Status(
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
            throttled,
            abs(Randomness.get().nextFloat()),
            randomBoolean() ? null : randomSimpleString(Randomness.get()),
            throttledUntil
        );
    }

    public static void assertEqualStatus(
        BulkByScrollTask.Status expected,
        BulkByScrollTask.Status actual,
        boolean includeUpdated,
        boolean includeCreated
    ) {
        assertNotSame(expected, actual);
        assertTrue(expected.equalsWithoutSliceStatus(actual, includeUpdated, includeCreated));
        assertThat(expected.getSliceStatuses().size(), equalTo(actual.getSliceStatuses().size()));
        for (int i = 0; i < expected.getSliceStatuses().size(); i++) {
            BulkByScrollTaskStatusOrExceptionTests.assertEqualStatusOrException(
                expected.getSliceStatuses().get(i),
                actual.getSliceStatuses().get(i),
                includeUpdated,
                includeCreated
            );
        }
    }

    @Override
    protected void assertEqualInstances(BulkByScrollTask.Status first, BulkByScrollTask.Status second) {
        assertEqualStatus(first, second, includeUpdated, includeCreated);
    }

    @Override
    protected BulkByScrollTask.Status createTestInstance() {
        // failures are tested separately, so we can test xcontent equivalence at least when we have no failures
        return randomStatusWithoutException();
    }

    @Override
    protected BulkByScrollTask.Status doParseInstance(XContentParser parser) throws IOException {
        return BulkByScrollTask.Status.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    /**
     * Test parsing {@link Status} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<Status> instanceSupplier = BulkByScrollTaskStatusTests::randomStatus;
        // with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        // but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        // exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(
            NUMBER_OF_TEST_RUNS,
            instanceSupplier,
            supportsUnknownFields,
            Strings.EMPTY_ARRAY,
            getRandomFieldsExcludeFilter(),
            this::createParser,
            this::doParseInstance,
            this::assertEqualInstances,
            assertToXContentEquivalence,
            ToXContent.EMPTY_PARAMS
        );
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            includeUpdated = false;
            params.put(Status.INCLUDE_UPDATED, "false");
        } else {
            includeUpdated = true;
        }
        if (randomBoolean()) {
            includeCreated = false;
            params.put(Status.INCLUDE_CREATED, "false");
        } else {
            includeCreated = true;
        }
        return new ToXContent.MapParams(params);
    }
}
