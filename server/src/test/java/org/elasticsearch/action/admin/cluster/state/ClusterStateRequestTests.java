/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Unit tests for the {@link ClusterStateRequest}.
 */
public class ClusterStateRequestTests extends ESTestCase {
    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {

            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            RemoteClusterStateRequest clusterStateRequest = new RemoteClusterStateRequest(TEST_REQUEST_TIMEOUT).routingTable(
                randomBoolean()
            )
                .metadata(randomBoolean())
                .nodes(randomBoolean())
                .blocks(randomBoolean())
                .indices("testindex", "testindex2")
                .indicesOptions(indicesOptions);

            TransportVersion testVersion = TransportVersionUtils.randomVersionBetween(
                random(),
                TransportVersions.MINIMUM_COMPATIBLE,
                TransportVersion.current()
            );
            if (randomBoolean()) {
                clusterStateRequest.waitForMetadataVersion(randomLongBetween(1, Long.MAX_VALUE));
            }
            if (randomBoolean()) {
                clusterStateRequest.waitForTimeout(new TimeValue(randomNonNegativeLong()));
            }

            BytesStreamOutput output = new BytesStreamOutput();
            output.setTransportVersion(testVersion);
            clusterStateRequest.writeTo(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setTransportVersion(testVersion);
            RemoteClusterStateRequest deserializedCSRequest = new RemoteClusterStateRequest(streamInput);

            assertThat(deserializedCSRequest.routingTable(), equalTo(clusterStateRequest.routingTable()));
            assertThat(deserializedCSRequest.metadata(), equalTo(clusterStateRequest.metadata()));
            assertThat(deserializedCSRequest.nodes(), equalTo(clusterStateRequest.nodes()));
            assertThat(deserializedCSRequest.blocks(), equalTo(clusterStateRequest.blocks()));
            assertThat(deserializedCSRequest.indices(), equalTo(clusterStateRequest.indices()));
            assertOptionsMatch(deserializedCSRequest.indicesOptions(), clusterStateRequest.indicesOptions());
            assertThat(deserializedCSRequest.waitForMetadataVersion(), equalTo(clusterStateRequest.waitForMetadataVersion()));
            assertThat(deserializedCSRequest.waitForTimeout(), equalTo(clusterStateRequest.waitForTimeout()));
        }
    }

    public void testWaitForMetadataVersion() {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest(TEST_REQUEST_TIMEOUT);
        expectThrows(
            IllegalArgumentException.class,
            () -> clusterStateRequest.waitForMetadataVersion(randomLongBetween(Long.MIN_VALUE, 0))
        );
        clusterStateRequest.waitForMetadataVersion(randomLongBetween(1, Long.MAX_VALUE));
    }

    private static void assertOptionsMatch(IndicesOptions in, IndicesOptions out) {
        assertThat(in.ignoreUnavailable(), equalTo(out.ignoreUnavailable()));
        assertThat(in.expandWildcardsClosed(), equalTo(out.expandWildcardsClosed()));
        assertThat(in.expandWildcardsOpen(), equalTo(out.expandWildcardsOpen()));
        assertThat(in.allowNoIndices(), equalTo(out.allowNoIndices()));
    }

    public void testDescription() {
        assertThat(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).clear().getDescription(),
            equalTo("cluster state [local, master timeout [30s]]")
        );
        assertThat(
            new ClusterStateRequest(TimeValue.timeValueMinutes(5)).getDescription(),
            equalTo("cluster state [routing table, nodes, metadata, blocks, customs, local, master timeout [5m]]")
        );
        assertThat(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).clear().routingTable(true).getDescription(),
            containsString("routing table")
        );
        assertThat(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).clear().nodes(true).getDescription(), containsString("nodes"));
        assertThat(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).clear().metadata(true).getDescription(), containsString("metadata"));
        assertThat(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).clear().blocks(true).getDescription(), containsString("blocks"));
        assertThat(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).clear().customs(true).getDescription(), containsString("customs"));
        assertThat(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(23L).getDescription(),
            containsString("wait for metadata version [23] with timeout [1m]")
        );
        assertThat(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).indices("foo", "bar").getDescription(),
            containsString("indices [foo, bar]")
        );
    }
}
