/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.transport;

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
        List<TransportAddress> transportAddressList = new ArrayList<>();
        for (InetAddress address : inetAddresses) {
            transportAddressList.add(new TransportAddress(address, randomIntBetween(9200, 9299)));
        }
        final BoundTransportAddress transportAddress = new BoundTransportAddress(
            transportAddressList.toArray(new TransportAddress[0]),
            transportAddressList.get(0)
        );
        assertThat(transportAddress.boundAddresses().length, equalTo(transportAddressList.size()));

        // serialize
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        transportAddress.writeTo(streamOutput);
        StreamInput in = streamOutput.bytes().streamInput();

        BoundTransportAddress serializedAddress = new BoundTransportAddress(in);

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
            new BoundTransportAddress(badArray, new TransportAddress(InetAddress.getLoopbackAddress(), 80));
            fail("expected an exception to be thrown due to no bound address");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
