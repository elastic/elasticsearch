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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.util.logging.Loggers;
import org.slf4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;

import static org.elasticsearch.util.MapBuilder.*;

/**
 * A global registry of all different types of {@link org.elasticsearch.util.transport.TransportAddress} allowing
 * to perfrom serialization of them.
 * <p/>
 * <p>By defualt, adds {@link org.elasticsearch.util.transport.InetSocketTransportAddress}.
 *
 * @author kimchy (Shay Banon)
 */
public abstract class TransportAddressSerializers {

    private static final Logger logger = Loggers.getLogger(TransportAddressSerializers.class);

    private static ImmutableMap<Short, Constructor<? extends TransportAddress>> addressConstructors = ImmutableMap.of();

    static {
        try {
            addAddressType(DummyTransportAddress.INSTANCE);
            addAddressType(new InetSocketTransportAddress());
        } catch (Exception e) {
            logger.warn("Failed to add InetSocketTransportAddress", e);
        }
    }

    public static synchronized void addAddressType(TransportAddress address) throws Exception {
        if (addressConstructors.containsKey(address.uniqueAddressTypeId())) {
            throw new ElasticSearchIllegalStateException("Address [" + address.uniqueAddressTypeId() + "] already bound");
        }
        Constructor<? extends TransportAddress> constructor = address.getClass().getDeclaredConstructor();
        constructor.setAccessible(true);
        addressConstructors = newMapBuilder(addressConstructors).put(address.uniqueAddressTypeId(), constructor).immutableMap();
    }

    public static TransportAddress addressFromStream(DataInput input) throws IOException, ClassNotFoundException {
        short addressUniqueId = input.readShort();
        Constructor<? extends TransportAddress> constructor = addressConstructors.get(addressUniqueId);
        if (constructor == null) {
            throw new IOException("No transport address mapped to [" + addressUniqueId + "]");
        }
        TransportAddress address;
        try {
            address = constructor.newInstance();
        } catch (Exception e) {
            throw new IOException("Failed to create class with constructor [" + constructor + "]", e);
        }
        address.readFrom(input);
        return address;
    }

    public static void addressToStream(DataOutput out, TransportAddress address) throws IOException {
        out.writeShort(address.uniqueAddressTypeId());
        address.writeTo(out);
    }
}
