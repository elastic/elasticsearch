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

package org.elasticsearch.common.transport;

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Basic tests for the {@link BoundTransportAddress} class. These tests should not bind to any addresses but should
 * just test things like serialization and exception handling...
 */
public class BoundTransportAddressTests extends ESTestCase {

    public void testSerialization() throws Exception {
        InetAddress[] inetAddresses = InetAddress.getAllByName("0.0.0.0");
        List<InetSocketTransportAddress> transportAddressList = new ArrayList<>();
        for (InetAddress address : inetAddresses) {
            transportAddressList.add(new InetSocketTransportAddress(address, randomIntBetween(9200, 9299)));
        }
        final BoundTransportAddress transportAddress =
            new BoundTransportAddress(transportAddressList.toArray(new InetSocketTransportAddress[0]), transportAddressList.get(0));
        assertThat(transportAddress.boundAddresses().length, equalTo(transportAddressList.size()));

        // serialize
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        transportAddress.writeTo(streamOutput);
        StreamInput in = ByteBufferStreamInput.wrap(streamOutput.bytes());

        BoundTransportAddress serializedAddress;
        if (randomBoolean()) {
            serializedAddress = BoundTransportAddress.readBoundTransportAddress(in);
        } else {
            serializedAddress = new BoundTransportAddress();
            serializedAddress.readFrom(in);
        }

        assertThat(serializedAddress, not(sameInstance(transportAddress)));
        assertThat(serializedAddress.boundAddresses().length, equalTo(transportAddress.boundAddresses().length));
        assertThat(serializedAddress.publishAddress(), equalTo(transportAddress.publishAddress()));

        TransportAddress[] serializedBoundAddresses = serializedAddress.boundAddresses();
        TransportAddress[] boundAddresses = transportAddress.boundAddresses();
        for (int i = 0; i < serializedBoundAddresses.length; i++) {
            assertThat(serializedBoundAddresses[i], equalTo(boundAddresses[i]));
        }
    }

    public void testBadBoundAddressArray() {
        try {
            TransportAddress[] badArray = randomBoolean() ? null : new TransportAddress[0];
            new BoundTransportAddress(badArray, new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), 80));
            fail("expected an exception to be thrown due to no bound address");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }
}
