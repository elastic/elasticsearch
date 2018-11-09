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
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class AddVotingTombstonesResponseTests extends ESTestCase {
    public void testSerialization() throws IOException {
        int tombstoneCount = between(0, 5);
        Set<DiscoveryNode> tombstones = new HashSet<>();
        while (tombstones.size() < tombstoneCount) {
            tombstones.add(new DiscoveryNode(randomAlphaOfLength(10), buildNewFakeTransportAddress(), Version.CURRENT));
        }
        final AddVotingTombstonesResponse originalRequest = new AddVotingTombstonesResponse(tombstones);
        final AddVotingTombstonesResponse deserialized
            = copyWriteable(originalRequest, writableRegistry(), AddVotingTombstonesResponse::new);
        assertThat(deserialized.getCurrentTombstones(), equalTo(originalRequest.getCurrentTombstones()));
    }
}
