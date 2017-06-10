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
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class BulkByScrollResponseTests extends ESTestCase {

    public void testRountTrip() throws IOException {
        BulkByScrollResponse response = new BulkByScrollResponse(timeValueMillis(randomNonNegativeLong()),
                BulkByScrollTaskStatusTests.randomStatus(), randomIndexingFailures(), randomSearchFailures(), randomBoolean());
        BulkByScrollResponse tripped = new BulkByScrollResponse();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                tripped.readFrom(in);
            }
        }
        assertResponseEquals(response, tripped);
    }

    private List<Failure> randomIndexingFailures() {
        return usually() ? emptyList()
                : singletonList(new Failure(randomSimpleString(random()), randomSimpleString(random()),
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
        return singletonList(new ScrollableHitSource.SearchFailure(new ElasticsearchException("foo"), index, shardId, nodeId));
    }

    private void assertResponseEquals(BulkByScrollResponse expected, BulkByScrollResponse actual) {
        assertEquals(expected.getTook(), actual.getTook());
        BulkByScrollTaskStatusTests.assertTaskStatusEquals(Version.CURRENT, expected.getStatus(), actual.getStatus());
        assertEquals(expected.getBulkFailures().size(), actual.getBulkFailures().size());
        for (int i = 0; i < expected.getBulkFailures().size(); i++) {
            Failure expectedFailure = expected.getBulkFailures().get(i);
            Failure actualFailure = actual.getBulkFailures().get(i);
            assertEquals(expectedFailure.getIndex(), actualFailure.getIndex());
            assertEquals(expectedFailure.getType(), actualFailure.getType());
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
        }
    }
}
