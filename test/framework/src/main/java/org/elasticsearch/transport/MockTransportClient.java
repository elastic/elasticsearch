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
package org.elasticsearch.transport;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.nio.NioTransportPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

@SuppressWarnings({"unchecked","varargs"})
public class MockTransportClient extends TransportClient {
    private static final Settings DEFAULT_SETTINGS = Settings.builder().put("transport.type.default",
        MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME).build();


    public MockTransportClient(Settings settings, Class<? extends Plugin>... plugins) {
        this(settings, Arrays.asList(plugins));
    }

    public MockTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        this(settings, plugins, null);
    }

    public MockTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins, HostFailureListener listener) {
        super(settings, DEFAULT_SETTINGS, addMockTransportIfMissing(settings, plugins), listener);
    }

    private static Collection<Class<? extends Plugin>> addMockTransportIfMissing(Settings settings,
                                                                                 Collection<Class<? extends Plugin>> plugins) {
        boolean settingExists = NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings);
        String transportType = NetworkModule.TRANSPORT_TYPE_SETTING.get(settings);
        if (settingExists == false || MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME.equals(transportType)) {
            if (plugins.contains(MockTcpTransportPlugin.class)) {
                return plugins;
            } else {
                plugins = new ArrayList<>(plugins);
                plugins.add(MockTcpTransportPlugin.class);
                return plugins;
            }
        } else if (NioTransportPlugin.NIO_TRANSPORT_NAME.equals(transportType)) {
            if (plugins.contains(NioTransportPlugin.class)) {
                return plugins;
            } else {
                plugins = new ArrayList<>(plugins);
                plugins.add(NioTransportPlugin.class);
                return plugins;
            }
        }
        return plugins;
    }

    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
