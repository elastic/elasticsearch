/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Ignore;

import java.util.Map;

@Ignore("Not a test")
public class RoutingAllocationTests extends ElasticsearchTestCase {

    public static DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, DummyTransportAddress.INSTANCE, Version.CURRENT);
    }

    public static DiscoveryNode newNode(String nodeId, TransportAddress address) {
        return new DiscoveryNode(nodeId, address, Version.CURRENT);
    }

    public static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode("", nodeId, DummyTransportAddress.INSTANCE, attributes, Version.CURRENT);
    }
}