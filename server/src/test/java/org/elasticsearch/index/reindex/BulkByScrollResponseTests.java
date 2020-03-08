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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.reindex.BulkByScrollTask.Status;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class BulkByScrollResponseTests extends AbstractXContentTestCase<BulkByScrollResponse> {

    private boolean includeUpdated;
    private boolean includeCreated;
    private boolean testExceptions = randomBoolean();

    public void testRountTrip() throws IOException {
        BulkByScrollResponse response = new BulkByScrollResponse(timeValueMillis(randomNonNegativeLong()),
                BulkByScrollTaskStatusTests.randomStatus(), randomIndexingFailures(), randomSearchFailures(), randomBoolean());
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
        return usually() ? emptyList()
                : singletonList(new Failure(randomSimpleString(random()),
                        randomSimpleString(random()), new IllegalArgumentException("test")));
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
        ElasticsearchException exception = randomFrom(new ResourceNotFoundException("bar"), new ElasticsearchException("foo"),
            new NoNodeAvailableException("baz"));
        return singletonList(new ScrollableHitSource.SearchFailure(exception, index, shardId, nodeId));
    }

    private void assertResponseEquals(BulkByScrollResponse expected, BulkByScrollResponse actual) {
        assertEquals(expected.getTook(), actual.getTook());
        BulkByScrollTaskStatusTests.assertTaskStatusEquals(Version.CURRENT, expected.getStatus(), actual.getStatus());
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

    public static void assertEqualBulkResponse(BulkByScrollResponse expected, BulkByScrollResponse actual, boolean includeUpdated,
                                               boolean includeCreated) {
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
            return new BulkByScrollResponse(timeValueMillis(randomNonNegativeLong()), BulkByScrollTaskStatusTests.randomStatus(),
                randomIndexingFailures(), randomSearchFailures(), randomBoolean());
        } else {
            return new BulkByScrollResponse(timeValueMillis(randomNonNegativeLong()),
                BulkByScrollTaskStatusTests.randomStatusWithoutException(), emptyList(), emptyList(), randomBoolean());
        }
    }

    @Override
    protected BulkByScrollResponse doParseInstance(XContentParser parser) throws IOException {
        return BulkByScrollResponse.fromXContent(parser);
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
