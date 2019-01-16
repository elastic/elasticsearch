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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class GetDiscoveredNodesRequestTests extends ESTestCase {

    public void testTimeoutValidation() {
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        assertThat("default value is 30s", getDiscoveredNodesRequest.getTimeout(), is(TimeValue.timeValueSeconds(30)));

        final TimeValue newTimeout = TimeValue.parseTimeValue(randomTimeValue(), "timeout");
        getDiscoveredNodesRequest.setTimeout(newTimeout);
        assertThat("value updated", getDiscoveredNodesRequest.getTimeout(), equalTo(newTimeout));

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> getDiscoveredNodesRequest.setTimeout(TimeValue.timeValueNanos(randomLongBetween(-10, -1))));
        assertThat(exception.getMessage(), startsWith("negative timeout of "));
        assertThat(exception.getMessage(), endsWith(" is not allowed"));

        getDiscoveredNodesRequest.setTimeout(null);
        assertThat("value updated", getDiscoveredNodesRequest.getTimeout(), nullValue());
    }

    public void testSerialization() throws IOException {
        final GetDiscoveredNodesRequest originalRequest = new GetDiscoveredNodesRequest();

        if (randomBoolean()) {
            originalRequest.setTimeout(TimeValue.parseTimeValue(randomTimeValue(), "timeout"));
        } else if (randomBoolean()) {
            originalRequest.setTimeout(null);
        }

        final GetDiscoveredNodesRequest deserialized = copyWriteable(originalRequest, writableRegistry(), GetDiscoveredNodesRequest::new);

        assertThat(deserialized.getTimeout(), equalTo(originalRequest.getTimeout()));
    }
}
