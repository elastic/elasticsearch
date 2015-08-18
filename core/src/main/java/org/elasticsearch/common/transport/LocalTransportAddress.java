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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.local.LocalTransport;

import java.io.IOException;

/**
 *
 */
public final class LocalTransportAddress implements TransportAddress {

    public static final LocalTransportAddress PROTO = new LocalTransportAddress("_na");

    private String id;

    public LocalTransportAddress(StreamInput in) throws IOException {
        id = in.readString();
    }

    public LocalTransportAddress(String id) {
        this.id = id;
    }

    public String id() {
        return this.id;
    }

    @Override
    public short uniqueAddressTypeId() {
        return 2;
    }

    @Override
    public boolean sameHost(TransportAddress other) {
        return other instanceof LocalTransportAddress && id.equals(((LocalTransportAddress) other).id);
    }

    @Override
    public String getHost() {
        return "local";
    }

    @Override
    public String getAddress() {
        return "0.0.0.0"; // see https://en.wikipedia.org/wiki/0.0.0.0
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public LocalTransportAddress readFrom(StreamInput in) throws IOException {
        return new LocalTransportAddress(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalTransportAddress that = (LocalTransportAddress) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "local[" + id + "]";
    }
}
