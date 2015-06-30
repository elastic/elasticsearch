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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.lang.reflect.Constructor;

import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 * A global registry of all different types of {@link org.elasticsearch.common.transport.TransportAddress} allowing
 * to perform serialization of them.
 * <p/>
 * <p>By default, adds {@link org.elasticsearch.common.transport.InetSocketTransportAddress}.
 *
 *
 */
public abstract class TransportAddressSerializers {

    private static final ESLogger logger = Loggers.getLogger(TransportAddressSerializers.class);

    private static ImmutableMap<Short, TransportAddress> ADDRESS_REGISTRY = ImmutableMap.of();

    static {
        try {
            addAddressType(DummyTransportAddress.INSTANCE);
            addAddressType(InetSocketTransportAddress.PROTO);
            addAddressType(LocalTransportAddress.PROTO);
        } catch (Exception e) {
            logger.warn("Failed to add InetSocketTransportAddress", e);
        }
    }

    public static synchronized void addAddressType(TransportAddress address) throws Exception {
        if (ADDRESS_REGISTRY.containsKey(address.uniqueAddressTypeId())) {
            throw new IllegalStateException("Address [" + address.uniqueAddressTypeId() + "] already bound");
        }
        ADDRESS_REGISTRY = newMapBuilder(ADDRESS_REGISTRY).put(address.uniqueAddressTypeId(), address).immutableMap();
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
