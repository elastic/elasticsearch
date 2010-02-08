/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.transport;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author kimchy (Shay Banon)
 */
public class InetSocketTransportAddress implements TransportAddress {

    private InetSocketAddress address;

    InetSocketTransportAddress() {

    }

    public InetSocketTransportAddress(String hostname, int port) {
        this(new InetSocketAddress(hostname, port));
    }

    public InetSocketTransportAddress(InetSocketAddress address) {
        this.address = address;
    }

    public static InetSocketTransportAddress readInetSocketTransportAddress(DataInput in) throws IOException, ClassNotFoundException {
        InetSocketTransportAddress address = new InetSocketTransportAddress();
        address.readFrom(in);
        return address;
    }

    @Override public short uniqueAddressTypeId() {
        return 1;
    }

    public InetSocketAddress address() {
        return this.address;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        address = new InetSocketAddress(in.readUTF(), in.readInt());
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(address.getHostName());
        out.writeInt(address.getPort());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InetSocketTransportAddress address1 = (InetSocketTransportAddress) o;

        if (address != null ? !address.equals(address1.address) : address1.address != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return address != null ? address.hashCode() : 0;
    }

    @Override public String toString() {
        return "inet[" + address + "]";
    }
}
