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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;

public class ReindexTaskStateDocTests extends AbstractXContentTestCase<ReindexTaskStateDoc> {
    @Override
    protected ReindexTaskStateDoc createTestInstance() {
        ReindexTaskStateDoc doc = new ReindexTaskStateDoc(new ReindexRequest().setSourceIndices("source").setDestIndex("dest"),
            randomBoolean());

        if (randomBoolean()) {
            doc = doc.withNewAllocation(randomNonNegativeLong(), new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()));
        }

        if (randomBoolean()) {
            doc = doc.withCheckpoint(new ScrollableHitSource.Checkpoint(randomNonNegativeLong()),
                randomStatus());
        }

        if (randomBoolean()) {
            doc = doc.withRequestsPerSecond(randomFloat());
        }

        if (randomBoolean()) {
            doc = doc.withFinishedState(new BulkByScrollResponse(TimeValue.timeValueMillis(randomLong()), randomStatus(),
                Collections.emptyList(), Collections.emptyList(), randomBoolean()), null);
        } else if (randomBoolean()) {
            // todo: need fixing once #49278 is merged.
//            doc = doc.withFinishedState(null, new ElasticsearchException("test"));
        }

        return  doc;
    }

    private BulkByScrollTask.Status randomStatus() {
        return new BulkByScrollTask.Status(null, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
            randomNonNegativeLong(), randomIntBetween(0, 1000), randomNonNegativeLong(), randomNonNegativeLong(),
            randomNonNegativeLong(), randomNonNegativeLong(), TimeValue.timeValueMillis(randomLong()),
            randomFloat(), randomBoolean() ? randomAlphaOfLength(10) : null, TimeValue.timeValueMillis(randomLong()));
    }

    @Override
    protected ReindexTaskStateDoc doParseInstance(XContentParser parser) throws IOException {
        return ReindexTaskStateDoc.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(ReindexTaskStateDoc expectedInstance, ReindexTaskStateDoc newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertArrayEquals(expectedInstance.getReindexRequest().getSearchRequest().indices(),
            newInstance.getReindexRequest().getSearchRequest().indices());
        // todo: once ReindexRequestTests are moved to reindex module, we can validate this properly.
        assertEquals(expectedInstance.getReindexRequest().getDestination().index(),
            newInstance.getReindexRequest().getDestination().index());
        assertEquals(expectedInstance.getAllocationId(), newInstance.getAllocationId());
        if (expectedInstance.getCheckpoint() != null) {
            assertEquals(expectedInstance.getCheckpoint().getRestartFromValue(), newInstance.getCheckpoint().getRestartFromValue());
        } else {
            assertNull(newInstance.getCheckpoint());
        }
        assertEquals(expectedInstance.getEphemeralTaskId(), newInstance.getEphemeralTaskId());
        if (expectedInstance.getException() != null) {
            assertEquals(expectedInstance.getException().getMessage(), newInstance.getException().getMessage());
        } else {
            assertNull(newInstance.getException());
        }
        assertEquals(expectedInstance.getFailureStatusCode(), newInstance.getFailureStatusCode());
        assertEquals(expectedInstance.getRequestsPerSecond(), newInstance.getRequestsPerSecond(), 0.0001d);
        // todo: once BulkByScrollResponseTests is moved to reindex module, we can validate this properly.
        if (expectedInstance.getReindexResponse() != null) {
            assertEquals(expectedInstance.getReindexResponse().getCreated(), newInstance.getReindexResponse().getCreated());
        } else {
            assertNull(newInstance.getReindexResponse());
        }
        assertEquals(expectedInstance.isResilient(), newInstance.isResilient());
    }
}
