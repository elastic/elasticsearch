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
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * A transport address used for IP socket address (wraps {@link java.net.InetSocketAddress}).
 */
public final class TransportAddress implements Writeable, ToXContentFragment {

    /**
     * A <a href="https://en.wikipedia.org/wiki/0.0.0.0">non-routeable v4 meta transport address</a> that can be used for
     * testing or in scenarios where targets should be marked as non-applicable from a transport perspective.
     */
    public static final InetAddress META_ADDRESS;

    static {
        try {
            META_ADDRESS = InetAddress.getByName("0.0.0.0");
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    private final InetSocketAddress address;

    public TransportAddress(InetAddress address, int port) {
        this(new InetSocketAddress(address, port));
    }

    public TransportAddress(InetSocketAddress address) {
        if (address == null) {
            throw new IllegalArgumentException("InetSocketAddress must not be null");
        }
        if (address.getAddress() == null) {
            throw new IllegalArgumentException("Address must be resolved but wasn't - InetSocketAddress#getAddress() returned null");
        }
        this.address = address;
    }

    /**
     * Read from a stream.
     */
    public TransportAddress(StreamInput in) throws IOException {
        final int len = in.readByte();
        final byte[] a = new byte[len]; // 4 bytes (IPv4) or 16 bytes (IPv6)
        in.readFully(a);
        String host = in.readString(); // the host string was serialized so we can ignore the passed in version
        final InetAddress inetAddress = InetAddress.getByAddress(host, a);
        int port = in.readInt();
        this.address = new InetSocketAddress(inetAddress, port);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        byte[] bytes = address.getAddress().getAddress();  // 4 bytes (IPv4) or 16 bytes (IPv6)
        out.writeByte((byte) bytes.length); // 1 byte
        out.write(bytes, 0, bytes.length);
        out.writeString(address.getHostString());
        // don't serialize scope ids over the network!!!!
        // these only make sense with respect to the local machine, and will only formulate
        // the address incorrectly remotely.
        out.writeInt(address.getPort());
    }

    /**
     * Returns a string representation of the enclosed {@link InetSocketAddress}
     * @see NetworkAddress#format(InetAddress)
     */
    public String getAddress() {
        return NetworkAddress.format(address.getAddress());
    }

    /**
     * Returns the addresses port
     */
    public int getPort() {
        return address.getPort();
    }

    /**
     * Returns the enclosed {@link InetSocketAddress}
     */
    public InetSocketAddress address() {
        return this.address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransportAddress address1 = (TransportAddress) o;
        return address.equals(address1.address);
    }

    @Override
    public int hashCode() {
        return address != null ? address.hashCode() : 0;
    }

    @Override
    public String toString() {
        return NetworkAddress.format(address);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }
}
