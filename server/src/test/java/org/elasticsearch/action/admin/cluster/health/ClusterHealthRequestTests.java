/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

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
