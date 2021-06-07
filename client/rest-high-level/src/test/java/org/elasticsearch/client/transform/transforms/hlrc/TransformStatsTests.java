/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.transforms.NodeAttributes;
import org.elasticsearch.client.transform.transforms.TransformCheckpointStats;
import org.elasticsearch.client.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.client.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.client.transform.transforms.TransformIndexerStats;
import org.elasticsearch.client.transform.transforms.TransformProgress;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TransformStatsTests extends AbstractResponseTestCase<
    org.elasticsearch.xpack.core.transform.transforms.TransformStats,
    org.elasticsearch.client.transform.transforms.TransformStats> {

    public static org.elasticsearch.xpack.core.transform.transforms.NodeAttributes randomNodeAttributes() {
        int numberOfAttributes = randomIntBetween(1, 10);
        Map<String, String> attributes = new HashMap<>(numberOfAttributes);
        for (int i = 0; i < numberOfAttributes; i++) {
            String val = randomAlphaOfLength(10);
            attributes.put("key-" + i, val);
        }
        return new org.elasticsearch.xpack.core.transform.transforms.NodeAttributes(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            attributes
        );
    }

    public static void assertHlrcEquals(
        org.elasticsearch.xpack.core.transform.transforms.TransformStats serverTestInstance,
        TransformStats clientInstance
    ) {
        assertThat(serverTestInstance.getId(), equalTo(clientInstance.getId()));
        assertThat(serverTestInstance.getState().value(), equalTo(clientInstance.getState().value()));
        assertTransformIndexerStats(serverTestInstance.getIndexerStats(), clientInstance.getIndexerStats());
        assertTransformCheckpointInfo(serverTestInstance.getCheckpointingInfo(), clientInstance.getCheckpointingInfo());
        assertNodeAttributes(serverTestInstance.getNode(), clientInstance.getNode());
        assertThat(serverTestInstance.getReason(), equalTo(clientInstance.getReason()));
    }

    @Override
    protected org.elasticsearch.xpack.core.transform.transforms.TransformStats createServerTestInstance(XContentType xContentType) {
        return new org.elasticsearch.xpack.core.transform.transforms.TransformStats(
            randomAlphaOfLength(10),
            randomFrom(org.elasticsearch.xpack.core.transform.transforms.TransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : randomNodeAttributes(),
            TransformIndexerStatsTests.randomStats(),
            TransformCheckpointingInfoTests.randomTransformCheckpointingInfo()
        );
    }

    @Override
    protected TransformStats doParseToClientInstance(XContentParser parser) throws IOException {
        return TransformStats.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.transform.transforms.TransformStats serverTestInstance,
        TransformStats clientInstance
    ) {
        assertHlrcEquals(serverTestInstance, clientInstance);
    }

    private static void assertNodeAttributes(
        org.elasticsearch.xpack.core.transform.transforms.NodeAttributes serverTestInstance,
        NodeAttributes clientInstance
    ) {
        if (serverTestInstance == null || clientInstance == null) {
            assertNull(serverTestInstance);
            assertNull(clientInstance);
            return;
        }
        assertThat(serverTestInstance.getAttributes(), equalTo(clientInstance.getAttributes()));
        assertThat(serverTestInstance.getEphemeralId(), equalTo(clientInstance.getEphemeralId()));
        assertThat(serverTestInstance.getId(), equalTo(clientInstance.getId()));
        assertThat(serverTestInstance.getName(), equalTo(clientInstance.getName()));
        assertThat(serverTestInstance.getTransportAddress(), equalTo(clientInstance.getTransportAddress()));
    }

    public static void assertTransformProgress(
        org.elasticsearch.xpack.core.transform.transforms.TransformProgress serverTestInstance,
        TransformProgress clientInstance
    ) {
        if (serverTestInstance == null || clientInstance == null) {
            assertNull(serverTestInstance);
            assertNull(clientInstance);
            return;
        }
        assertThat(serverTestInstance.getPercentComplete(), equalTo(clientInstance.getPercentComplete()));
        assertThat(serverTestInstance.getDocumentsProcessed(), equalTo(clientInstance.getDocumentsProcessed()));
        assertThat(serverTestInstance.getTotalDocs(), equalTo(clientInstance.getTotalDocs()));
        assertThat(serverTestInstance.getDocumentsIndexed(), equalTo(clientInstance.getDocumentsIndexed()));
    }

    public static void assertPosition(
        org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition serverTestInstance,
        TransformIndexerPosition clientInstance
    ) {
        assertThat(serverTestInstance.getIndexerPosition(), equalTo(clientInstance.getIndexerPosition()));
        assertThat(serverTestInstance.getBucketsPosition(), equalTo(clientInstance.getBucketsPosition()));
    }

    public static void assertTransformCheckpointStats(
        org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats serverTestInstance,
        TransformCheckpointStats clientInstance
    ) {
        assertTransformProgress(serverTestInstance.getCheckpointProgress(), clientInstance.getCheckpointProgress());
        assertThat(serverTestInstance.getCheckpoint(), equalTo(clientInstance.getCheckpoint()));
        assertPosition(serverTestInstance.getPosition(), clientInstance.getPosition());
        assertThat(serverTestInstance.getTimestampMillis(), equalTo(clientInstance.getTimestampMillis()));
        assertThat(serverTestInstance.getTimeUpperBoundMillis(), equalTo(clientInstance.getTimeUpperBoundMillis()));
    }

    public static void assertTransformCheckpointInfo(
        org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo serverTestInstance,
        TransformCheckpointingInfo clientInstance
    ) {
        assertTransformCheckpointStats(serverTestInstance.getNext(), clientInstance.getNext());
        assertTransformCheckpointStats(serverTestInstance.getLast(), clientInstance.getLast());
        assertThat(serverTestInstance.getChangesLastDetectedAt(), equalTo(clientInstance.getChangesLastDetectedAt()));
        assertThat(serverTestInstance.getOperationsBehind(), equalTo(clientInstance.getOperationsBehind()));
    }

    public static void assertTransformIndexerStats(
        org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats serverTestInstance,
        TransformIndexerStats clientInstance
    ) {
        assertThat(serverTestInstance.getExpAvgCheckpointDurationMs(), equalTo(clientInstance.getExpAvgCheckpointDurationMs()));
        assertThat(serverTestInstance.getExpAvgDocumentsProcessed(), equalTo(clientInstance.getExpAvgDocumentsProcessed()));
        assertThat(serverTestInstance.getExpAvgDocumentsIndexed(), equalTo(clientInstance.getExpAvgDocumentsIndexed()));
        assertThat(serverTestInstance.getNumPages(), equalTo(clientInstance.getPagesProcessed()));
        assertThat(serverTestInstance.getIndexFailures(), equalTo(clientInstance.getIndexFailures()));
        assertThat(serverTestInstance.getIndexTime(), equalTo(clientInstance.getIndexTime()));
        assertThat(serverTestInstance.getIndexTotal(), equalTo(clientInstance.getIndexTotal()));
        assertThat(serverTestInstance.getNumDocuments(), equalTo(clientInstance.getDocumentsProcessed()));
        assertThat(serverTestInstance.getNumInvocations(), equalTo(clientInstance.getTriggerCount()));
        assertThat(serverTestInstance.getOutputDocuments(), equalTo(clientInstance.getDocumentsIndexed()));
        assertThat(serverTestInstance.getNumDeletedDocuments(), equalTo(clientInstance.getDocumentsDeleted()));
        assertThat(serverTestInstance.getSearchFailures(), equalTo(clientInstance.getSearchFailures()));
        assertThat(serverTestInstance.getSearchTime(), equalTo(clientInstance.getSearchTime()));
        assertThat(serverTestInstance.getSearchTotal(), equalTo(clientInstance.getSearchTotal()));
        assertThat(serverTestInstance.getDeleteTime(), equalTo(clientInstance.getDeleteTime()));
    }
}
