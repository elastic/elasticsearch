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

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.netty4.Netty4Transport;

import java.util.Arrays;
import java.util.List;

public class Netty4Plugin extends Plugin {

    public static final String NETTY_TRANSPORT_NAME = "netty4";
    public static final String NETTY_HTTP_TRANSPORT_NAME = "netty4";

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_MAX_CUMULATION_BUFFER_CAPACITY,
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty4HttpServerTransport.SETTING_HTTP_WORKER_COUNT,
            Netty4HttpServerTransport.SETTING_HTTP_TCP_NO_DELAY,
            Netty4HttpServerTransport.SETTING_HTTP_TCP_KEEP_ALIVE,
            Netty4HttpServerTransport.SETTING_HTTP_TCP_BLOCKING_SERVER,
            Netty4HttpServerTransport.SETTING_HTTP_TCP_REUSE_ADDRESS,
            Netty4HttpServerTransport.SETTING_HTTP_TCP_SEND_BUFFER_SIZE,
            Netty4HttpServerTransport.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE,
            Netty4Transport.WORKER_COUNT,
            Netty4Transport.NETTY_MAX_CUMULATION_BUFFER_CAPACITY,
            Netty4Transport.NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_MIN,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_MAX,
            Netty4Transport.NETTY_BOSS_COUNT
        );
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
                // here we set the netty4 transport and http transport as the default. This is a set once setting
                // ie. if another plugin does that as well the server will fail - only one default network can exist!
                .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), NETTY_HTTP_TRANSPORT_NAME)
                .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), NETTY_TRANSPORT_NAME)
                .build();
    }

    public void onModule(NetworkModule networkModule) {
        if (networkModule.canRegisterHttpExtensions()) {
            networkModule.registerHttpTransport(NETTY_HTTP_TRANSPORT_NAME, Netty4HttpServerTransport.class);
        }
        networkModule.registerTransport(NETTY_TRANSPORT_NAME, Netty4Transport.class);
    }

}
