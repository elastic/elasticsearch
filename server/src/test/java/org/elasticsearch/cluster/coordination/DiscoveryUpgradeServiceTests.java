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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

public class DiscoveryUpgradeServiceTests extends ESTestCase {
    public void testCreateDiscoveryNodeWithImpossiblyHighId() {
        final DiscoveryNode discoveryNode
            = new DiscoveryNode(UUIDs.randomBase64UUID(random()), buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode fakeNode = DiscoveryUpgradeService.createDiscoveryNodeWithImpossiblyHighId(discoveryNode);
        assertThat(discoveryNode.getId(), lessThan(fakeNode.getId()));
        assertThat(UUIDs.randomBase64UUID(random()), lessThan(fakeNode.getId()));
        assertThat(fakeNode.getId(), containsString(discoveryNode.getId()));
    }
}
