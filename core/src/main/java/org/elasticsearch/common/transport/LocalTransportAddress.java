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

import java.io.IOException;

/**
 *
 */
public final class LocalTransportAddress implements TransportAddress {
    public static final short TYPE_ID = 2;

    private String id;

    public LocalTransportAddress(String id) {
        this.id = id;
    }

    /**
     * Read from a stream.
     */
    public LocalTransportAddress(StreamInput in) throws IOException {
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    public String id() {
        return this.id;
    }

    @Override
    public short uniqueAddressTypeId() {
        return TYPE_ID;
    }

    @Override
    public boolean sameHost(TransportAddress other) {
        return other instanceof LocalTransportAddress && id.equals(((LocalTransportAddress) other).id);
    }

    @Override
    public boolean isLoopbackOrLinkLocalAddress() {
        return false;
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
