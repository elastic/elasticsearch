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

package org.elasticsearch.usage;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UsageServiceTests extends ESTestCase {

    public void testRestUsage() {
        DiscoveryNode discoveryNode = new DiscoveryNode("foo", new LocalTransportAddress("bar"), Version.CURRENT);
        UsageService usageService = new UsageService(() -> discoveryNode, Settings.EMPTY);
        usageService.addRestCall("a");
        usageService.addRestCall("b");
        usageService.addRestCall("a");
        usageService.addRestCall("a");
        usageService.addRestCall("b");
        usageService.addRestCall("c");
        usageService.addRestCall("c");
        usageService.addRestCall("d");
        usageService.addRestCall("a");
        usageService.addRestCall("b");
        usageService.addRestCall("e");
        usageService.addRestCall("f");
        usageService.addRestCall("c");
        usageService.addRestCall("d");
        NodeUsage usage = usageService.getUsageStats(true);
        assertThat(usage.getNode(), sameInstance(discoveryNode));
        Map<String, Long> restUsage = usage.getRestUsage();
        assertThat(restUsage, notNullValue());
        assertThat(restUsage.size(), equalTo(6));
        assertThat(restUsage.get("a"), equalTo(4L));
        assertThat(restUsage.get("b"), equalTo(3L));
        assertThat(restUsage.get("c"), equalTo(3L));
        assertThat(restUsage.get("d"), equalTo(2L));
        assertThat(restUsage.get("e"), equalTo(1L));
        assertThat(restUsage.get("f"), equalTo(1L));

        usage = usageService.getUsageStats(false);
        assertThat(usage.getNode(), sameInstance(discoveryNode));
        assertThat(usage.getRestUsage(), nullValue());
    }

    public void testClearUsage() {
        DiscoveryNode discoveryNode = new DiscoveryNode("foo", new LocalTransportAddress("bar"), Version.CURRENT);
        UsageService usageService = new UsageService(() -> discoveryNode, Settings.EMPTY);
        usageService.addRestCall("a");
        usageService.addRestCall("b");
        usageService.addRestCall("c");
        usageService.addRestCall("d");
        usageService.addRestCall("e");
        usageService.addRestCall("f");
        NodeUsage usage = usageService.getUsageStats(true);
        assertThat(usage.getNode(), sameInstance(discoveryNode));
        Map<String, Long> restUsage = usage.getRestUsage();
        assertThat(restUsage, notNullValue());
        assertThat(restUsage.size(), equalTo(6));
        assertThat(restUsage.get("a"), equalTo(1L));
        assertThat(restUsage.get("b"), equalTo(1L));
        assertThat(restUsage.get("c"), equalTo(1L));
        assertThat(restUsage.get("d"), equalTo(1L));
        assertThat(restUsage.get("e"), equalTo(1L));
        assertThat(restUsage.get("f"), equalTo(1L));

        usageService.clear();
        usage = usageService.getUsageStats(true);
        assertThat(usage.getNode(), sameInstance(discoveryNode));
        assertThat(usage.getRestUsage(), notNullValue());
        assertThat(usage.getRestUsage().size(), equalTo(0));
    }

}
