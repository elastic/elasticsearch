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
import org.elasticsearch.common.io.ThrowableObjectInputStream;
import org.elasticsearch.common.io.ThrowableObjectOutputStream;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.VersionUtils.randomVersion;

public class DiscoveryNodeTests extends ElasticsearchTestCase {


    @Test
    public void testJavaSerializablilty() throws IOException, ClassNotFoundException {
        final int iters = scaledRandomIntBetween(100, 300);
        for (int i = 0; i < iters; i++) {
            final String id = randomUnicodeOfLengthBetween(3, 20);
            final String nodeName = randomUnicodeOfLengthBetween(3, 20);
            final String hostName = randomUnicodeOfLengthBetween(3, 20);
            final String hostAddress = randomUnicodeOfLengthBetween(3, 20);
            final TransportAddress transportAddress = new LocalTransportAddress(randomUnicodeOfLengthBetween(3, 20));
            final Map<String, String> attributes = new HashMap<>();
            for (int a = randomInt(10); a > 0; a--) {
                attributes.put(randomUnicodeOfLengthBetween(3, 20), randomUnicodeOfLengthBetween(3, 20));
            }
            final Version version = randomVersion(random());
            DiscoveryNode discoveryNode = new DiscoveryNode(nodeName, id, hostName, hostAddress, transportAddress, attributes, version);
            BytesStreamOutput bytesOutput = new BytesStreamOutput();
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(bytesOutput);
            too.writeObject(discoveryNode);
            too.close();
            ThrowableObjectInputStream from = new ThrowableObjectInputStream(new BytesStreamInput(bytesOutput.bytes()));
            DiscoveryNode readDiscoveryNode = (DiscoveryNode) from.readObject();
            from.close();
            assertThat(readDiscoveryNode, Matchers.equalTo(discoveryNode));
            assertThat(readDiscoveryNode.id(), Matchers.equalTo(id));
            assertThat(readDiscoveryNode.name(), Matchers.equalTo(nodeName));
            assertThat(readDiscoveryNode.getHostName(), Matchers.equalTo(hostName));
            assertThat(readDiscoveryNode.getHostAddress(), Matchers.equalTo(hostAddress));
            assertThat(readDiscoveryNode.address(), Matchers.equalTo(transportAddress));
            assertThat(readDiscoveryNode.attributes(), Matchers.equalTo(attributes));
            assertThat(readDiscoveryNode.version(), Matchers.equalTo(version));
        }
    }

}
