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
public class DummyTransportAddress implements TransportAddress {

    public static final DummyTransportAddress INSTANCE = new DummyTransportAddress();

    private DummyTransportAddress() {
    }

    @Override
    public short uniqueAddressTypeId() {
        return 0;
    }

    @Override
    public boolean sameHost(TransportAddress other) {
        return other == INSTANCE;
    }

    @Override
    public String getHost() {
        return "dummy";
    }

    @Override
    public String getAddress() {
        return "0.0.0.0"; // see https://en.wikipedia.org/wiki/0.0.0.0
    }

    @Override
    public int getPort() {
        return 42;
    }

    @Override
    public DummyTransportAddress readFrom(StreamInput in) throws IOException {
        return INSTANCE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String toString() {
        return "_dummy_addr_";
    }
}
