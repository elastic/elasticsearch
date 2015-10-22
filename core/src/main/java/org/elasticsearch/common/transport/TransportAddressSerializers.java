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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * A global registry of all different types of {@link org.elasticsearch.common.transport.TransportAddress} allowing
 * to perform serialization of them.
 * <p>
 * By default, adds {@link org.elasticsearch.common.transport.InetSocketTransportAddress}.
 *
 *
 */
public abstract class TransportAddressSerializers {

    private static final ESLogger logger = Loggers.getLogger(TransportAddressSerializers.class);

    private static final Map<Short, TransportAddress> ADDRESS_REGISTRY;

    static {
        Map<Short, TransportAddress> registry = new HashMap<>();
        try {
            addAddressType(registry, DummyTransportAddress.INSTANCE);
            addAddressType(registry, InetSocketTransportAddress.PROTO);
            addAddressType(registry, LocalTransportAddress.PROTO);
        } catch (Exception e) {
            logger.warn("Failed to setup TransportAddresses", e);
        }
        ADDRESS_REGISTRY = unmodifiableMap(registry);
    }

    public static synchronized void addAddressType(Map<Short, TransportAddress> registry, TransportAddress address) throws Exception {
        if (registry.containsKey(address.uniqueAddressTypeId())) {
            throw new IllegalStateException("Address [" + address.uniqueAddressTypeId() + "] already bound");
        }
        registry.put(address.uniqueAddressTypeId(), address);
    }

    public static TransportAddress addressFromStream(StreamInput input) throws IOException {
        short addressUniqueId = input.readShort();
        TransportAddress addressType = ADDRESS_REGISTRY.get(addressUniqueId);
        if (addressType == null) {
            throw new IOException("No transport address mapped to [" + addressUniqueId + "]");
        }
        return addressType.readFrom(input);
    }

    public static void addressToStream(StreamOutput out, TransportAddress address) throws IOException {
        out.writeShort(address.uniqueAddressTypeId());
        address.writeTo(out);
    }
}
