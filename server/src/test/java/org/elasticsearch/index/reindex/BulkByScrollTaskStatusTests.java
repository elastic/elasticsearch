/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.reindex.BulkByScrollTask.Status;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParseException;
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
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
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
        XContentParser.Token token;
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        } else {
            token = parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return innerParseStatus(parser);
    }

    private static final ConstructingObjectParser<Tuple<Long, Long>, Void> RETRIES_PARSER = new ConstructingObjectParser<>(
        "bulk_by_scroll_task_status_retries",
        true,
        a -> new Tuple<>(((Long) a[0]), (Long) a[1])
    );
    static {
        RETRIES_PARSER.declareLong(constructorArg(), new ParseField(BulkByScrollTask.Status.RETRIES_BULK_FIELD));
        RETRIES_PARSER.declareLong(constructorArg(), new ParseField(BulkByScrollTask.Status.RETRIES_SEARCH_FIELD));
    }

    public static void declareFields(ObjectParser<? extends BulkByScrollTask.StatusBuilder, Void> parser) {
        parser.declareInt(BulkByScrollTask.StatusBuilder::setSliceId, new ParseField(BulkByScrollTask.Status.SLICE_ID_FIELD));
        parser.declareLong(BulkByScrollTask.StatusBuilder::setTotal, new ParseField(BulkByScrollTask.Status.TOTAL_FIELD));
        parser.declareLong(BulkByScrollTask.StatusBuilder::setUpdated, new ParseField(BulkByScrollTask.Status.UPDATED_FIELD));
        parser.declareLong(BulkByScrollTask.StatusBuilder::setCreated, new ParseField(BulkByScrollTask.Status.CREATED_FIELD));
        parser.declareLong(BulkByScrollTask.StatusBuilder::setDeleted, new ParseField(BulkByScrollTask.Status.DELETED_FIELD));
        parser.declareInt(BulkByScrollTask.StatusBuilder::setBatches, new ParseField(BulkByScrollTask.Status.BATCHES_FIELD));
        parser.declareLong(
            BulkByScrollTask.StatusBuilder::setVersionConflicts,
            new ParseField(BulkByScrollTask.Status.VERSION_CONFLICTS_FIELD)
        );
        parser.declareLong(BulkByScrollTask.StatusBuilder::setNoops, new ParseField(BulkByScrollTask.Status.NOOPS_FIELD));
        parser.declareObject(
            BulkByScrollTask.StatusBuilder::setRetries,
            RETRIES_PARSER,
            new ParseField(BulkByScrollTask.Status.RETRIES_FIELD)
        );
        parser.declareLong(BulkByScrollTask.StatusBuilder::setThrottled, new ParseField(BulkByScrollTask.Status.THROTTLED_RAW_FIELD));
        parser.declareFloat(
            BulkByScrollTask.StatusBuilder::setRequestsPerSecond,
            new ParseField(BulkByScrollTask.Status.REQUESTS_PER_SEC_FIELD)
        );
        parser.declareString(BulkByScrollTask.StatusBuilder::setReasonCancelled, new ParseField(BulkByScrollTask.Status.CANCELED_FIELD));
        parser.declareLong(
            BulkByScrollTask.StatusBuilder::setThrottledUntil,
            new ParseField(BulkByScrollTask.Status.THROTTLED_UNTIL_RAW_FIELD)
        );
        parser.declareObjectArray(
            BulkByScrollTask.StatusBuilder::setSliceStatuses,
            (p, c) -> parseStatusOrException(p),
            new ParseField(BulkByScrollTask.Status.SLICES_FIELD)
        );
    }

    private static Status innerParseStatus(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String fieldName = parser.currentName();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        BulkByScrollTask.StatusBuilder builder = new BulkByScrollTask.StatusBuilder();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName.equals(Status.RETRIES_FIELD)) {
                    builder.setRetries(RETRIES_PARSER.parse(parser, null));
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (fieldName.equals(Status.SLICES_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        builder.addToSliceStatuses(parseStatusOrException(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else { // else if it is a value
                switch (fieldName) {
                    case Status.SLICE_ID_FIELD -> builder.setSliceId(parser.intValue());
                    case Status.TOTAL_FIELD -> builder.setTotal(parser.longValue());
                    case Status.UPDATED_FIELD -> builder.setUpdated(parser.longValue());
                    case Status.CREATED_FIELD -> builder.setCreated(parser.longValue());
                    case Status.DELETED_FIELD -> builder.setDeleted(parser.longValue());
                    case Status.BATCHES_FIELD -> builder.setBatches(parser.intValue());
                    case Status.VERSION_CONFLICTS_FIELD -> builder.setVersionConflicts(parser.longValue());
                    case Status.NOOPS_FIELD -> builder.setNoops(parser.longValue());
                    case Status.THROTTLED_RAW_FIELD -> builder.setThrottled(parser.longValue());
                    case Status.REQUESTS_PER_SEC_FIELD -> builder.setRequestsPerSecond(parser.floatValue());
                    case Status.CANCELED_FIELD -> builder.setReasonCancelled(parser.text());
                    case Status.THROTTLED_UNTIL_RAW_FIELD -> builder.setThrottledUntil(parser.longValue());
                }
            }
        }
        return builder.buildStatus();
    }

    /**
     * Since {@link BulkByScrollTask.StatusOrException} can contain either an {@link Exception} or a {@link Status} we need to peek
     * at a field first before deciding what needs to be parsed since the same object could contains either.
     * The {@link BulkByScrollTask.StatusOrException#EXPECTED_EXCEPTION_FIELDS} contains the fields that are expected when the serialised
     * object was an instance of exception and the {@link Status#FIELDS_SET} is the set of fields expected when the
     * serialized object was an instance of Status.
     */
    public static BulkByScrollTask.StatusOrException parseStatusOrException(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            token = parser.nextToken();
            // This loop is present only to ignore unknown tokens. It breaks as soon as we find a field
            // that is allowed.
            while (token != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                String fieldName = parser.currentName();
                // weird way to ignore unknown tokens
                if (Status.FIELDS_SET.contains(fieldName)) {
                    return new BulkByScrollTask.StatusOrException(innerParseStatus(parser));
                } else if (BulkByScrollTask.StatusOrException.EXPECTED_EXCEPTION_FIELDS.contains(fieldName)) {
                    return new BulkByScrollTask.StatusOrException(ElasticsearchException.innerFromXContent(parser, false));
                } else {
                    // Ignore unknown tokens
                    token = parser.nextToken();
                    if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                        parser.skipChildren();
                    }
                    token = parser.nextToken();
                }
            }
            throw new XContentParseException("Unable to parse StatusFromException. Expected fields not found.");
        }
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
