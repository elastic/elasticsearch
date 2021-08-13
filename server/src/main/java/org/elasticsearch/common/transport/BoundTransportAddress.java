/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.InetAddresses;

import java.io.IOException;

/**
 * A bounded transport address is a tuple of {@link TransportAddress}, one array that represents
 * the addresses the transport is bound to, and the other is the published one that represents the address clients
 * should communicate on.
 */
public class BoundTransportAddress implements Writeable {

    private TransportAddress[] boundAddresses;

    private TransportAddress publishAddress;

    public BoundTransportAddress(StreamInput in) throws IOException {
        int boundAddressLength = in.readInt();
        boundAddresses = new TransportAddress[boundAddressLength];
        for (int i = 0; i < boundAddressLength; i++) {
            boundAddresses[i] = new TransportAddress(in);
        }
        publishAddress = new TransportAddress(in);
    }

    public BoundTransportAddress(TransportAddress[] boundAddresses, TransportAddress publishAddress) {
        if (boundAddresses == null || boundAddresses.length < 1) {
            throw new IllegalArgumentException("at least one bound address must be provided");
        }
        this.boundAddresses = boundAddresses;
        this.publishAddress = publishAddress;
    }

    public TransportAddress[] boundAddresses() {
        return boundAddresses;
    }

    public TransportAddress publishAddress() {
        return publishAddress;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(boundAddresses.length);
        for (TransportAddress address : boundAddresses) {
            address.writeTo(out);
        }
        publishAddress.writeTo(out);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("publish_address {");
        String hostString = publishAddress.address().getHostString();
        String publishAddressString = publishAddress.toString();
        if (InetAddresses.isInetAddress(hostString) == false) {
            publishAddressString = hostString + '/' + publishAddress.toString();
        }
        builder.append(publishAddressString);
        builder.append("}, bound_addresses ");
        boolean firstAdded = false;
        for (TransportAddress address : boundAddresses) {
            if (firstAdded) {
                builder.append(", ");
            } else {
                firstAdded = true;
            }

            builder.append("{").append(address).append("}");
        }
        return builder.toString();
    }
}
