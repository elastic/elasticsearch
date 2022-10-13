/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

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
            ClusterStateRequest clusterStateRequest = new ClusterStateRequest().routingTable(randomBoolean())
                .metadata(randomBoolean())
                .nodes(randomBoolean())
                .blocks(randomBoolean())
                .indices("testindex", "testindex2")
                .indicesOptions(indicesOptions);

            Version testVersion = VersionUtils.randomVersionBetween(
                random(),
                Version.CURRENT.minimumCompatibilityVersion(),
                Version.CURRENT
            );
            // TODO: change version to V_6_6_0 after backporting:
            if (testVersion.onOrAfter(Version.V_7_0_0)) {
                if (randomBoolean()) {
                    clusterStateRequest.waitForMetadataVersion(randomLongBetween(1, Long.MAX_VALUE));
                }
                if (randomBoolean()) {
                    clusterStateRequest.waitForTimeout(new TimeValue(randomNonNegativeLong()));
                }
            }

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(testVersion);
            clusterStateRequest.writeTo(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(testVersion);
            ClusterStateRequest deserializedCSRequest = new ClusterStateRequest(streamInput);

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
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
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
        assertThat(new ClusterStateRequest().clear().getDescription(), equalTo("cluster state [master timeout [30s]]"));
        assertThat(
            new ClusterStateRequest().masterNodeTimeout("5m").getDescription(),
            equalTo("cluster state [routing table, nodes, metadata, blocks, customs, master timeout [5m]]")
        );
        assertThat(new ClusterStateRequest().clear().routingTable(true).getDescription(), containsString("routing table"));
        assertThat(new ClusterStateRequest().clear().nodes(true).getDescription(), containsString("nodes"));
        assertThat(new ClusterStateRequest().clear().metadata(true).getDescription(), containsString("metadata"));
        assertThat(new ClusterStateRequest().clear().blocks(true).getDescription(), containsString("blocks"));
        assertThat(new ClusterStateRequest().clear().customs(true).getDescription(), containsString("customs"));
        assertThat(new ClusterStateRequest().local(true).getDescription(), containsString("local"));
        assertThat(
            new ClusterStateRequest().waitForMetadataVersion(23L).getDescription(),
            containsString("wait for metadata version [23] with timeout [1m]")
        );
        assertThat(new ClusterStateRequest().indices("foo", "bar").getDescription(), containsString("indices [foo, bar]"));
    }
}
