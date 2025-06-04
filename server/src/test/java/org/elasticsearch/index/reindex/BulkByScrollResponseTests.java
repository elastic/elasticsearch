/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.reindex.BulkByScrollTask.Status;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.core.TimeValue.timeValueMillis;

public class BulkByScrollResponseTests extends AbstractXContentTestCase<BulkByScrollResponse> {

    private static final ObjectParser<BulkByScrollResponseBuilder, Void> PARSER = new ObjectParser<>(
        "bulk_by_scroll_response",
        true,
        BulkByScrollResponseBuilder::new
    );
    static {
        PARSER.declareLong(BulkByScrollResponseBuilder::setTook, new ParseField(BulkByScrollResponse.TOOK_FIELD));
        PARSER.declareBoolean(BulkByScrollResponseBuilder::setTimedOut, new ParseField(BulkByScrollResponse.TIMED_OUT_FIELD));
        PARSER.declareObjectArray(
            BulkByScrollResponseBuilder::setFailures,
            (p, c) -> parseFailure(p),
            new ParseField(BulkByScrollResponse.FAILURES_FIELD)
        );
        // since the result of BulkByScrollResponse.Status are mixed we also parse that in this
        BulkByScrollTaskStatusTests.declareFields(PARSER);
    }

    private static Object parseFailure(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParser.Token token;
        String index = null;
        String id = null;
        Integer status = null;
        Integer shardId = null;
        String nodeId = null;
        ElasticsearchException bulkExc = null;
        ElasticsearchException searchExc = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String name = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else if (token == XContentParser.Token.START_OBJECT) {
                switch (name) {
                    case ScrollableHitSource.SearchFailure.REASON_FIELD -> searchExc = ElasticsearchException.fromXContent(parser);
                    case Failure.CAUSE_FIELD -> bulkExc = ElasticsearchException.fromXContent(parser);
                    default -> parser.skipChildren();
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                switch (name) {
                    // This field is the same as SearchFailure.index
                    case Failure.INDEX_FIELD -> index = parser.text();
                    case Failure.ID_FIELD -> id = parser.text();
                    case ScrollableHitSource.SearchFailure.NODE_FIELD -> nodeId = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                switch (name) {
                    case Failure.STATUS_FIELD -> status = parser.intValue();
                    case ScrollableHitSource.SearchFailure.SHARD_FIELD -> shardId = parser.intValue();
                }
            }
        }
        if (bulkExc != null) {
            return new Failure(index, id, bulkExc, RestStatus.fromCode(status));
        } else if (searchExc != null) {
            if (status == null) {
                return new ScrollableHitSource.SearchFailure(searchExc, index, shardId, nodeId);
            } else {
                return new ScrollableHitSource.SearchFailure(searchExc, index, shardId, nodeId, RestStatus.fromCode(status));
            }
        } else {
            throw new ElasticsearchParseException("failed to parse failures array. At least one of {reason,cause} must be present");
        }
    }

    private boolean includeUpdated;
    private boolean includeCreated;
    private boolean testExceptions = randomBoolean();

    public void testRountTrip() throws IOException {
        BulkByScrollResponse response = new BulkByScrollResponse(
            timeValueMillis(randomNonNegativeLong()),
            BulkByScrollTaskStatusTests.randomStatus(),
            randomIndexingFailures(),
            randomSearchFailures(),
            randomBoolean()
        );
        BulkByScrollResponse tripped;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                tripped = new BulkByScrollResponse(in);
            }
        }
        assertResponseEquals(response, tripped);
    }

    private List<Failure> randomIndexingFailures() {
        return usually()
            ? emptyList()
            : singletonList(new Failure(randomSimpleString(random()), randomSimpleString(random()), new IllegalArgumentException("test")));
    }

    private List<ScrollableHitSource.SearchFailure> randomSearchFailures() {
        if (randomBoolean()) {
            return emptyList();
        }
        String index = null;
        Integer shardId = null;
        String nodeId = null;
        if (randomBoolean()) {
            index = randomAlphaOfLength(5);
            shardId = randomInt();
            nodeId = usually() ? randomAlphaOfLength(5) : null;
        }
        ElasticsearchException exception = randomFrom(
            new ResourceNotFoundException("bar"),
            new ElasticsearchException("foo"),
            new NoNodeAvailableException("baz")
        );
        return singletonList(new ScrollableHitSource.SearchFailure(exception, index, shardId, nodeId));
    }

    private void assertResponseEquals(BulkByScrollResponse expected, BulkByScrollResponse actual) {
        assertEquals(expected.getTook(), actual.getTook());
        BulkByScrollTaskStatusTests.assertTaskStatusEquals(TransportVersion.current(), expected.getStatus(), actual.getStatus());
        assertEquals(expected.getBulkFailures().size(), actual.getBulkFailures().size());
        for (int i = 0; i < expected.getBulkFailures().size(); i++) {
            Failure expectedFailure = expected.getBulkFailures().get(i);
            Failure actualFailure = actual.getBulkFailures().get(i);
            assertEquals(expectedFailure.getIndex(), actualFailure.getIndex());
            assertEquals(expectedFailure.getId(), actualFailure.getId());
            assertEquals(expectedFailure.getMessage(), actualFailure.getMessage());
            assertEquals(expectedFailure.getStatus(), actualFailure.getStatus());
        }
        assertEquals(expected.getSearchFailures().size(), actual.getSearchFailures().size());
        for (int i = 0; i < expected.getSearchFailures().size(); i++) {
            ScrollableHitSource.SearchFailure expectedFailure = expected.getSearchFailures().get(i);
            ScrollableHitSource.SearchFailure actualFailure = actual.getSearchFailures().get(i);
            assertEquals(expectedFailure.getIndex(), actualFailure.getIndex());
            assertEquals(expectedFailure.getShardId(), actualFailure.getShardId());
            assertEquals(expectedFailure.getNodeId(), actualFailure.getNodeId());
            assertEquals(expectedFailure.getReason().getClass(), actualFailure.getReason().getClass());
            assertEquals(expectedFailure.getReason().getMessage(), actualFailure.getReason().getMessage());
            assertEquals(expectedFailure.getStatus(), actualFailure.getStatus());
        }
    }

    public static void assertEqualBulkResponse(
        BulkByScrollResponse expected,
        BulkByScrollResponse actual,
        boolean includeUpdated,
        boolean includeCreated
    ) {
        assertEquals(expected.getTook(), actual.getTook());
        BulkByScrollTaskStatusTests.assertEqualStatus(expected.getStatus(), actual.getStatus(), includeUpdated, includeCreated);
        assertEquals(expected.getBulkFailures().size(), actual.getBulkFailures().size());
        for (int i = 0; i < expected.getBulkFailures().size(); i++) {
            Failure expectedFailure = expected.getBulkFailures().get(i);
            Failure actualFailure = actual.getBulkFailures().get(i);
            assertEquals(expectedFailure.getIndex(), actualFailure.getIndex());
            assertEquals(expectedFailure.getId(), actualFailure.getId());
            assertEquals(expectedFailure.getStatus(), actualFailure.getStatus());
        }
        assertEquals(expected.getSearchFailures().size(), actual.getSearchFailures().size());
        for (int i = 0; i < expected.getSearchFailures().size(); i++) {
            ScrollableHitSource.SearchFailure expectedFailure = expected.getSearchFailures().get(i);
            ScrollableHitSource.SearchFailure actualFailure = actual.getSearchFailures().get(i);
            assertEquals(expectedFailure.getIndex(), actualFailure.getIndex());
            assertEquals(expectedFailure.getShardId(), actualFailure.getShardId());
            assertEquals(expectedFailure.getNodeId(), actualFailure.getNodeId());
            assertEquals(expectedFailure.getStatus(), actualFailure.getStatus());
        }
    }

    @Override
    protected void assertEqualInstances(BulkByScrollResponse expected, BulkByScrollResponse actual) {
        assertEqualBulkResponse(expected, actual, includeUpdated, includeCreated);
    }

    @Override
    protected BulkByScrollResponse createTestInstance() {
        if (testExceptions) {
            return new BulkByScrollResponse(
                timeValueMillis(randomNonNegativeLong()),
                BulkByScrollTaskStatusTests.randomStatus(),
                randomIndexingFailures(),
                randomSearchFailures(),
                randomBoolean()
            );
        } else {
            return new BulkByScrollResponse(
                timeValueMillis(randomNonNegativeLong()),
                BulkByScrollTaskStatusTests.randomStatusWithoutException(),
                emptyList(),
                emptyList(),
                randomBoolean()
            );
        }
    }

    @Override
    protected BulkByScrollResponse doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null).buildResponse();
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        // XContentEquivalence fails in the exception case, due to how exceptions are serialized.
        return testExceptions == false;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
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
