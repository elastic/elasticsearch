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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class GetDiscoveredNodesRequestTests extends ESTestCase {
    public void testWaitForNodesValidation() {
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        assertThat("default value is 1", getDiscoveredNodesRequest.waitForNodes(), is(1));
        assertNull("default is valid", getDiscoveredNodesRequest.validate());

        final int newWaitForNodes = randomIntBetween(1, 10);
        getDiscoveredNodesRequest.waitForNodes(newWaitForNodes);
        assertThat("value updated", getDiscoveredNodesRequest.waitForNodes(), is(newWaitForNodes));
        assertNull("updated request is still valid", getDiscoveredNodesRequest.validate());

        final IllegalArgumentException exception
            = expectThrows(IllegalArgumentException.class, () -> getDiscoveredNodesRequest.waitForNodes(randomIntBetween(-10, 0)));
        assertThat(exception.getMessage(), startsWith("always finds at least one node, waiting for "));
        assertThat(exception.getMessage(), endsWith(" is not allowed"));
    }

    public void testTimeoutValidation() {
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        assertThat("default value is zero", getDiscoveredNodesRequest.timeout(), is(TimeValue.ZERO));
        assertNull(getDiscoveredNodesRequest.validate());

        final TimeValue newTimeout = TimeValue.parseTimeValue(randomTimeValue(), "timeout");
        getDiscoveredNodesRequest.timeout(newTimeout);
        assertThat("value updated", getDiscoveredNodesRequest.timeout(), equalTo(newTimeout));

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> getDiscoveredNodesRequest.timeout(TimeValue.timeValueNanos(randomLongBetween(-10, -1))));
        assertThat(exception.getMessage(), startsWith("negative timeout of "));
        assertThat(exception.getMessage(), endsWith(" is not allowed"));
    }

    public void testNoTimeoutAcceptedIfNoNodesToAwait() {
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest
            = new GetDiscoveredNodesRequest().waitForNodes(1).timeout(TimeValue.parseTimeValue(randomPositiveTimeValue(), "timeout"));
        final ActionRequestValidationException exception = getDiscoveredNodesRequest.validate();
        assertThat(exception.validationErrors(), hasSize(1));
        final String validationError = exception.validationErrors().get(0);
        assertThat(validationError, startsWith("always discovers at least one node, so a timeout of "));
        assertThat(validationError, endsWith(" is unnecessary"));
    }

    public void testTimeoutAcceptedIfNodesToAwait() {
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest()
            .waitForNodes(randomIntBetween(2, 10)).timeout(TimeValue.parseTimeValue(randomPositiveTimeValue(), "timeout"));
        assertNull(getDiscoveredNodesRequest.validate());
    }

    public void testSerialization() throws IOException {
        final GetDiscoveredNodesRequest originalRequest = new GetDiscoveredNodesRequest();

        if (randomBoolean()) {
            originalRequest.waitForNodes(randomIntBetween(1, 10));
        }

        if (randomBoolean()) {
            originalRequest.timeout(TimeValue.parseTimeValue(randomTimeValue(), "timeout"));
        }

        final GetDiscoveredNodesRequest deserialized = copyWriteable(originalRequest, writableRegistry(),
            Streamable.newWriteableReader(GetDiscoveredNodesRequest::new));

        assertThat(deserialized.waitForNodes(), equalTo(originalRequest.waitForNodes()));
        assertThat(deserialized.timeout(), equalTo(originalRequest.timeout()));
    }
}
