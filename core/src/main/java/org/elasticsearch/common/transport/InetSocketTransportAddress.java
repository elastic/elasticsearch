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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * A transport address used for IP socket address (wraps {@link java.net.InetSocketAddress}).
 */
public final class InetSocketTransportAddress implements TransportAddress {
    public static final short TYPE_ID = 1;

    private final InetSocketAddress address;

    public InetSocketTransportAddress(InetAddress address, int port) {
        this(new InetSocketAddress(address, port));
    }

    public InetSocketTransportAddress(InetSocketAddress address) {
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
    public InetSocketTransportAddress(StreamInput in) throws IOException {
        this(in, null);
    }

    /**
     * Read from a stream and use the {@code hostString} when creating the InetAddress if the input comes from a version on or prior
     * {@link Version#V_5_0_2} as the hostString was not serialized
     */
    public InetSocketTransportAddress(StreamInput in, String hostString) throws IOException {
        final int len = in.readByte();
        final byte[] a = new byte[len]; // 4 bytes (IPv4) or 16 bytes (IPv6)
        in.readFully(a);
        final InetAddress inetAddress;
        if (in.getVersion().after(Version.V_5_0_2)) {
            String host = in.readString();
            inetAddress = InetAddress.getByAddress(host, a); // the host string was serialized so we can ignore the passed in value
        } else {
            // prior to this version, we did not serialize the host string so we used the passed in value
            inetAddress = InetAddress.getByAddress(hostString, a);
        }
        int port = in.readInt();
        this.address = new InetSocketAddress(inetAddress, port);
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        byte[] bytes = address().getAddress().getAddress();  // 4 bytes (IPv4) or 16 bytes (IPv6)
        out.writeByte((byte) bytes.length); // 1 byte
        out.write(bytes, 0, bytes.length);
        if (out.getVersion().after(Version.V_5_0_2)) {
            out.writeString(address.getHostString());
        }
        // don't serialize scope ids over the network!!!!
        // these only make sense with respect to the local machine, and will only formulate
        // the address incorrectly remotely.
        out.writeInt(address.getPort());
    }

    @Override
    public short uniqueAddressTypeId() {
        return TYPE_ID;
    }

    @Override
    public boolean sameHost(TransportAddress other) {
        return other instanceof InetSocketTransportAddress &&
                address.getAddress().equals(((InetSocketTransportAddress) other).address.getAddress());
    }

    @Override
    public boolean isLoopbackAddress() {
        return address.getAddress().isLoopbackAddress();
    }

    @Override
    public String getHost() {
       return address.getHostString(); // just delegate no resolving done by getHostString
    }

    @Override
    public String getAddress() {
        return NetworkAddress.format(address.getAddress());
    }

    @Override
    public int getPort() {
        return address.getPort();
    }

    public InetSocketAddress address() {
        return this.address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InetSocketTransportAddress address1 = (InetSocketTransportAddress) o;
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
}
