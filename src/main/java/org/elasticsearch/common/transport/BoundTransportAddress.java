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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * A bounded transport address is a tuple of two {@link TransportAddress}, one that represents
 * the address the transport is bounded on, the the published one represents the one clients should
 * communicate on.
 *
 *
 */
public class BoundTransportAddress implements Streamable {

    private TransportAddress boundAddress;

    private TransportAddress publishAddress;

    BoundTransportAddress() {
    }

    public BoundTransportAddress(TransportAddress boundAddress, TransportAddress publishAddress) {
        this.boundAddress = boundAddress;
        this.publishAddress = publishAddress;
    }

    public TransportAddress boundAddress() {
        return boundAddress;
    }

    public TransportAddress publishAddress() {
        return publishAddress;
    }

    public static BoundTransportAddress readBoundTransportAddress(StreamInput in) throws IOException {
        BoundTransportAddress addr = new BoundTransportAddress();
        addr.readFrom(in);
        return addr;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        boundAddress = TransportAddressSerializers.addressFromStream(in);
        publishAddress = TransportAddressSerializers.addressFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAddressSerializers.addressToStream(out, boundAddress);
        TransportAddressSerializers.addressToStream(out, publishAddress);
    }

    @Override
    public String toString() {
        return "bound_address {" + boundAddress + "}, publish_address {" + publishAddress + "}";
    }
}
