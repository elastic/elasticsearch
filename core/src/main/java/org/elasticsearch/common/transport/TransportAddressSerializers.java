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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * A global registry of all supported types of {@link TransportAddress}s. This registry is not open for modification by plugins.
 */
public abstract class TransportAddressSerializers {
    private static final Map<Short, Writeable.Reader<TransportAddress>> ADDRESS_REGISTRY;

    static {
        Map<Short, Writeable.Reader<TransportAddress>> registry = new HashMap<>();
        addAddressType(registry, DummyTransportAddress.INSTANCE.uniqueAddressTypeId(), (in) -> DummyTransportAddress.INSTANCE);
        addAddressType(registry, InetSocketTransportAddress.TYPE_ID, InetSocketTransportAddress::new);
        addAddressType(registry, LocalTransportAddress.TYPE_ID, LocalTransportAddress::new);
        ADDRESS_REGISTRY = unmodifiableMap(registry);
    }

    private static void addAddressType(Map<Short, Writeable.Reader<TransportAddress>> registry, short uniqueAddressTypeId,
            Writeable.Reader<TransportAddress> address) {
        if (registry.containsKey(uniqueAddressTypeId)) {
            throw new IllegalStateException("Address [" + uniqueAddressTypeId + "] already bound");
        }
        registry.put(uniqueAddressTypeId, address);
    }

    public static TransportAddress addressFromStream(StreamInput input) throws IOException {
        // TODO why don't we just use named writeables here?
        short addressUniqueId = input.readShort();
        Writeable.Reader<TransportAddress> addressType = ADDRESS_REGISTRY.get(addressUniqueId);
        if (addressType == null) {
            throw new IOException("No transport address mapped to [" + addressUniqueId + "]");
        }
        return addressType.read(input);
    }

    public static void addressToStream(StreamOutput out, TransportAddress address) throws IOException {
        out.writeShort(address.uniqueAddressTypeId());
        address.writeTo(out);
    }
}
