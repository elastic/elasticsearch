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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.cluster.RemoteInfoRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ClusterRequestConvertersTests extends ESTestCase {

    public void testClusterPutSettings() throws IOException {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(request, expectedParams);
        RequestConvertersTests.setRandomTimeout(request::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        Request expectedRequest = ClusterRequestConverters.clusterPutSettings(request);
        Assert.assertEquals("/_cluster/settings", expectedRequest.getEndpoint());
        Assert.assertEquals(HttpPut.METHOD_NAME, expectedRequest.getMethod());
        Assert.assertEquals(expectedParams, expectedRequest.getParameters());
    }

    public void testClusterGetSettings() throws IOException {
        ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(request, expectedParams);
        request.includeDefaults(ESTestCase.randomBoolean());
        if (request.includeDefaults()) {
            expectedParams.put("include_defaults", String.valueOf(true));
        }

        Request expectedRequest = ClusterRequestConverters.clusterGetSettings(request);
        Assert.assertEquals("/_cluster/settings", expectedRequest.getEndpoint());
        Assert.assertEquals(HttpGet.METHOD_NAME, expectedRequest.getMethod());
        Assert.assertEquals(expectedParams, expectedRequest.getParameters());
    }

    public void testClusterHealth() {
        ClusterHealthRequest healthRequest = new ClusterHealthRequest();
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomLocal(healthRequest::local, expectedParams);
        String timeoutType = ESTestCase.randomFrom("timeout", "masterTimeout", "both", "none");
        String timeout = ESTestCase.randomTimeValue();
        String masterTimeout = ESTestCase.randomTimeValue();
        switch (timeoutType) {
            case "timeout":
                healthRequest.timeout(timeout);
                expectedParams.put("timeout", timeout);
                // If Master Timeout wasn't set it uses the same value as Timeout
                expectedParams.put("master_timeout", timeout);
                break;
            case "masterTimeout":
                expectedParams.put("timeout", "30s");
                healthRequest.masterNodeTimeout(masterTimeout);
                expectedParams.put("master_timeout", masterTimeout);
                break;
            case "both":
                healthRequest.timeout(timeout);
                expectedParams.put("timeout", timeout);
                healthRequest.masterNodeTimeout(timeout);
                expectedParams.put("master_timeout", timeout);
                break;
            case "none":
                expectedParams.put("timeout", "30s");
                expectedParams.put("master_timeout", "30s");
                break;
            default:
                throw new UnsupportedOperationException();
        }
        RequestConvertersTests.setRandomWaitForActiveShards(healthRequest::waitForActiveShards, ActiveShardCount.NONE, expectedParams);
        if (ESTestCase.randomBoolean()) {
            ClusterHealthRequest.Level level = ESTestCase.randomFrom(ClusterHealthRequest.Level.values());
            healthRequest.level(level);
            expectedParams.put("level", level.name().toLowerCase(Locale.ROOT));
        } else {
            expectedParams.put("level", "cluster");
        }
        if (ESTestCase.randomBoolean()) {
            Priority priority = ESTestCase.randomFrom(Priority.values());
            healthRequest.waitForEvents(priority);
            expectedParams.put("wait_for_events", priority.name().toLowerCase(Locale.ROOT));
        }
        if (ESTestCase.randomBoolean()) {
            ClusterHealthStatus status = ESTestCase.randomFrom(ClusterHealthStatus.values());
            healthRequest.waitForStatus(status);
            expectedParams.put("wait_for_status", status.name().toLowerCase(Locale.ROOT));
        }
        if (ESTestCase.randomBoolean()) {
            boolean waitForNoInitializingShards = ESTestCase.randomBoolean();
            healthRequest.waitForNoInitializingShards(waitForNoInitializingShards);
            if (waitForNoInitializingShards) {
                expectedParams.put("wait_for_no_initializing_shards", Boolean.TRUE.toString());
            }
        }
        if (ESTestCase.randomBoolean()) {
            boolean waitForNoRelocatingShards = ESTestCase.randomBoolean();
            healthRequest.waitForNoRelocatingShards(waitForNoRelocatingShards);
            if (waitForNoRelocatingShards) {
                expectedParams.put("wait_for_no_relocating_shards", Boolean.TRUE.toString());
            }
        }
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        healthRequest.indices(indices);

        Request request = ClusterRequestConverters.clusterHealth(healthRequest);
        Assert.assertThat(request, CoreMatchers.notNullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
        if (CollectionUtils.isEmpty(indices) == false) {
            Assert.assertThat(request.getEndpoint(), equalTo("/_cluster/health/" + String.join(",", indices)));
        } else {
            Assert.assertThat(request.getEndpoint(), equalTo("/_cluster/health"));
        }
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
    }

    public void testRemoteInfo() {
        RemoteInfoRequest request = new RemoteInfoRequest();
        Request expectedRequest = ClusterRequestConverters.remoteInfo(request);
        assertEquals("/_remote/info", expectedRequest.getEndpoint());
        assertEquals(HttpGet.METHOD_NAME, expectedRequest.getMethod());
        assertEquals(emptyMap(), expectedRequest.getParameters());
    }
}
