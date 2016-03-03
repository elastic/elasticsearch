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

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class DiscoveryNodeServiceTests extends ESTestCase {

    public void testClientNodeSettingIsProhibited() {
        Settings settings = Settings.builder().put("node.client", randomBoolean()).build();
        try {
            new DiscoveryNodeService(settings).buildAttributes();
            fail("build attributes should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("node.client setting is no longer supported, use node.master, " +
                    "node.data and node.ingest explicitly instead"));
        }
    }

    public void testBuildAttributesWithCustomAttributeServiceProvider() {
        Map<String, String> expectedAttributes = new HashMap<>();
        int numCustomSettings = randomIntBetween(0, 5);
        Settings.Builder builder = Settings.builder();
        for (int i = 0; i < numCustomSettings; i++) {
            builder.put("node.attr" + i, "value" + i);
            expectedAttributes.put("attr" + i, "value" + i);
        }
        DiscoveryNodeService discoveryNodeService = new DiscoveryNodeService(builder.build());
        int numCustomAttributes = randomIntBetween(0, 5);
        Map<String, String> customAttributes = new HashMap<>();
        for (int i = 0; i < numCustomAttributes; i++) {
            customAttributes.put("custom-" + randomAsciiOfLengthBetween(5, 10), randomAsciiOfLengthBetween(1, 10));
        }
        expectedAttributes.putAll(customAttributes);
        discoveryNodeService.addCustomAttributeProvider(() -> customAttributes);

        Map<String, String> attributes = discoveryNodeService.buildAttributes();
        assertThat(attributes, equalTo(expectedAttributes));
    }
}
