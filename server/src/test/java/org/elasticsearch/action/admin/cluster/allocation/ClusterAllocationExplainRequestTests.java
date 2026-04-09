/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

public class ClusterAllocationExplainRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest(
            randomTimeValue(),
            randomAlphaOfLength(4),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean(),
            randomBoolean() ? randomAlphaOfLength(5) : null
        );
        request.includeYesDecisions(randomBoolean());
        request.includeDiskInfo(randomBoolean());
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        ClusterAllocationExplainRequest actual = new ClusterAllocationExplainRequest(output.bytes().streamInput());
        assertEquals(request.masterNodeTimeout(), actual.masterNodeTimeout());
        assertEquals(request.getIndex(), actual.getIndex());
        assertEquals(request.getShard(), actual.getShard());
        assertEquals(request.isPrimary(), actual.isPrimary());
        assertEquals(request.includeYesDecisions(), actual.includeYesDecisions());
        assertEquals(request.includeDiskInfo(), actual.includeDiskInfo());
        assertEquals(request.getCurrentNode(), actual.getCurrentNode());
    }

    public void testToStringWithEmptyBody() {
        ClusterAllocationExplainRequest clusterAllocationExplainRequest = new ClusterAllocationExplainRequest(randomTimeValue());
        clusterAllocationExplainRequest.includeYesDecisions(true);
        clusterAllocationExplainRequest.includeDiskInfo(false);

        String expected = "ClusterAllocationExplainRequest[useAnyUnassignedShard=true,"
            + "include_yes_decisions?=true,include_disk_info?=false";
        assertEquals(expected, clusterAllocationExplainRequest.toString());
    }

    public void testToStringWithValidBodyButCurrentNodeIsNull() {
        String index = "test-index";
        int shard = randomInt();
        boolean primary = randomBoolean();
        ClusterAllocationExplainRequest clusterAllocationExplainRequest = new ClusterAllocationExplainRequest(
            randomTimeValue(),
            index,
            shard,
            primary,
            null
        );
        clusterAllocationExplainRequest.includeYesDecisions(false);
        clusterAllocationExplainRequest.includeDiskInfo(true);

        String expected = "ClusterAllocationExplainRequest[index="
            + index
            + ",shard="
            + shard
            + ",primary?="
            + primary
            + ",include_yes_decisions?=false"
            + ",include_disk_info?=true";
        assertEquals(expected, clusterAllocationExplainRequest.toString());
    }

    public void testToStringWithAllBodyParameters() {
        String index = "test-index";
        int shard = randomInt();
        boolean primary = randomBoolean();
        String currentNode = "current_node";
        ClusterAllocationExplainRequest clusterAllocationExplainRequest = new ClusterAllocationExplainRequest(
            randomTimeValue(),
            index,
            shard,
            primary,
            currentNode
        );
        clusterAllocationExplainRequest.includeYesDecisions(false);
        clusterAllocationExplainRequest.includeDiskInfo(true);

        String expected = "ClusterAllocationExplainRequest[index="
            + index
            + ",shard="
            + shard
            + ",primary?="
            + primary
            + ",current_node="
            + currentNode
            + ",include_yes_decisions?=false"
            + ",include_disk_info?=true";
        assertEquals(expected, clusterAllocationExplainRequest.toString());
    }
}
