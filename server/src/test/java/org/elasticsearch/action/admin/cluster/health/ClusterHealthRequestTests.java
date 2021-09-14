/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.Locale;

import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.core.IsEqual.equalTo;

public class ClusterHealthRequestTests extends ESTestCase {

    public void testSerialize() throws Exception {
        final ClusterHealthRequest originalRequest = randomRequest();
        final ClusterHealthRequest cloneRequest;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                cloneRequest = new ClusterHealthRequest(in);
            }
        }
        assertThat(cloneRequest.waitForStatus(), equalTo(originalRequest.waitForStatus()));
        assertThat(cloneRequest.waitForNodes(), equalTo(originalRequest.waitForNodes()));
        assertThat(cloneRequest.waitForNoInitializingShards(), equalTo(originalRequest.waitForNoInitializingShards()));
        assertThat(cloneRequest.waitForNoRelocatingShards(), equalTo(originalRequest.waitForNoRelocatingShards()));
        assertThat(cloneRequest.waitForActiveShards(), equalTo(originalRequest.waitForActiveShards()));
        assertThat(cloneRequest.waitForEvents(), equalTo(originalRequest.waitForEvents()));
        assertIndicesEquals(cloneRequest.indices(), originalRequest.indices());
        assertThat(cloneRequest.indicesOptions(), equalTo(originalRequest.indicesOptions()));
    }

    public void testRequestReturnsHiddenIndicesByDefault() {
        final ClusterHealthRequest defaultRequest = new ClusterHealthRequest();
        assertTrue(defaultRequest.indicesOptions().expandWildcardsHidden());
    }

    public void testBwcSerialization() throws Exception {
        for (int runs = 0; runs < randomIntBetween(5, 20); runs++) {
            // Generate a random cluster health request in version < 7.2.0 and serializes it
            final BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(randomVersionBetween(random(), VersionUtils.getFirstVersion(), getPreviousVersion(Version.V_7_2_0)));

            final ClusterHealthRequest expected = randomRequest();
            {
                expected.getParentTask().writeTo(out);
                out.writeTimeValue(expected.masterNodeTimeout());
                out.writeBoolean(expected.local());
                if (expected.indices() == null) {
                    out.writeVInt(0);
                } else {
                    out.writeVInt(expected.indices().length);
                    for (String index : expected.indices()) {
                        out.writeString(index);
                    }
                }
                out.writeTimeValue(expected.timeout());
                if (expected.waitForStatus() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeByte(expected.waitForStatus().value());
                }
                out.writeBoolean(expected.waitForNoRelocatingShards());
                expected.waitForActiveShards().writeTo(out);
                out.writeString(expected.waitForNodes());
                if (expected.waitForEvents() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    Priority.writeTo(expected.waitForEvents(), out);
                }
                out.writeBoolean(expected.waitForNoInitializingShards());
            }

            // Deserialize and check the cluster health request
            final StreamInput in = out.bytes().streamInput();
            in.setVersion(out.getVersion());
            final ClusterHealthRequest actual = new ClusterHealthRequest(in);

            assertThat(actual.waitForStatus(), equalTo(expected.waitForStatus()));
            assertThat(actual.waitForNodes(), equalTo(expected.waitForNodes()));
            assertThat(actual.waitForNoInitializingShards(), equalTo(expected.waitForNoInitializingShards()));
            assertThat(actual.waitForNoRelocatingShards(), equalTo(expected.waitForNoRelocatingShards()));
            assertThat(actual.waitForActiveShards(), equalTo(expected.waitForActiveShards()));
            assertThat(actual.waitForEvents(), equalTo(expected.waitForEvents()));
            assertIndicesEquals(actual.indices(), expected.indices());
            assertThat(actual.indicesOptions(), equalTo(IndicesOptions.lenientExpandOpen()));
        }

        for (int runs = 0; runs < randomIntBetween(5, 20); runs++) {
            // Generate a random cluster health request in current version
            final ClusterHealthRequest expected = randomRequest();

            // Serialize to node in version < 7.2.0
            final BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(randomVersionBetween(random(), VersionUtils.getFirstVersion(), getPreviousVersion(Version.V_7_2_0)));
            expected.writeTo(out);

            // Deserialize and check the cluster health request
            final StreamInput in = out.bytes().streamInput();
            in.setVersion(out.getVersion());
            final ClusterHealthRequest actual = new ClusterHealthRequest(in);

            assertThat(actual.waitForStatus(), equalTo(expected.waitForStatus()));
            assertThat(actual.waitForNodes(), equalTo(expected.waitForNodes()));
            assertThat(actual.waitForNoInitializingShards(), equalTo(expected.waitForNoInitializingShards()));
            assertThat(actual.waitForNoRelocatingShards(), equalTo(expected.waitForNoRelocatingShards()));
            assertThat(actual.waitForActiveShards(), equalTo(expected.waitForActiveShards()));
            assertThat(actual.waitForEvents(), equalTo(expected.waitForEvents()));
            assertIndicesEquals(actual.indices(), expected.indices());
            assertThat(actual.indicesOptions(), equalTo(IndicesOptions.lenientExpandOpen()));
        }
    }

    private ClusterHealthRequest randomRequest() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.waitForStatus(randomFrom(ClusterHealthStatus.values()));
        request.waitForNodes(randomFrom("", "<", "<=", ">", ">=") + between(0, 1000));
        request.waitForNoInitializingShards(randomBoolean());
        request.waitForNoRelocatingShards(randomBoolean());
        request.waitForActiveShards(randomIntBetween(0, 10));
        request.waitForEvents(randomFrom(Priority.values()));
        if (randomBoolean()) {
            final String[] indices = new String[randomIntBetween(1, 10)];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            }
            request.indices(indices);
        }
        if (randomBoolean()) {
            request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        return request;
    }

    private static void assertIndicesEquals(final String[] actual, final String[] expected) {
        // null indices in ClusterHealthRequest is deserialized as empty string array
        assertArrayEquals(expected != null ? expected : Strings.EMPTY_ARRAY, actual);
    }
}
