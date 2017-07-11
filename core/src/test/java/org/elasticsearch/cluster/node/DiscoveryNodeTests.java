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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class DiscoveryNodeTests extends ESTestCase {

    public void testDiscoveryNodeIsCreatedWithHostFromInetAddress() throws Exception {
        InetAddress inetAddress = randomBoolean() ? InetAddress.getByName("192.0.2.1") :
            InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);
        assertEquals(transportAddress.address().getHostString(), node.getHostName());
        assertEquals(transportAddress.getAddress(), node.getHostAddress());
    }

    public void testDiscoveryNodeSerializationKeepsHost() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.CURRENT);
        node.writeTo(streamOutput);

        StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
        DiscoveryNode serialized = new DiscoveryNode(in);
        assertEquals(transportAddress.address().getHostString(), serialized.getHostName());
        assertEquals(transportAddress.address().getHostString(), serialized.getAddress().address().getHostString());
        assertEquals(transportAddress.getAddress(), serialized.getHostAddress());
        assertEquals(transportAddress.getAddress(), serialized.getAddress().getAddress());
        assertEquals(transportAddress.getPort(), serialized.getAddress().getPort());
    }

    public void testDiscoveryNodeSerializationToOldVersion() throws Exception {
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode node = new DiscoveryNode("name1", "id1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.setVersion(Version.V_5_0_0);
        node.writeTo(streamOutput);

        StreamInput in = StreamInput.wrap(streamOutput.bytes().toBytesRef().bytes);
        in.setVersion(Version.V_5_0_0);
        DiscoveryNode serialized = new DiscoveryNode(in);
        assertEquals(transportAddress.address().getHostString(), serialized.getHostName());
        assertEquals(transportAddress.address().getHostString(), serialized.getAddress().address().getHostString());
        assertEquals(transportAddress.getAddress(), serialized.getHostAddress());
        assertEquals(transportAddress.getAddress(), serialized.getAddress().getAddress());
        assertEquals(transportAddress.getPort(), serialized.getAddress().getPort());
        assertFalse("if the minimum index compatibility version moves past 5.0.3, remove the special casing in DiscoverNode(StreamInput)" +
                " and the TransportAddress(StreamInput, String) constructor",
            Version.CURRENT.minimumIndexCompatibilityVersion().after(Version.V_5_0_2));
        // serialization can happen from an old cluster-state in a full cluster restart
        // hence we need to maintain this until we drop index bwc
    }
}
