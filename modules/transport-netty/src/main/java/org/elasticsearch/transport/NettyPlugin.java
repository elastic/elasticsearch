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
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.Arrays;
import java.util.List;

public class NettyPlugin extends Plugin {

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            NettyHttpServerTransport.SETTING_HTTP_NETTY_MAX_CUMULATION_BUFFER_CAPACITY,
            NettyHttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            NettyHttpServerTransport.SETTING_HTTP_WORKER_COUNT,
            NettyHttpServerTransport.SETTING_HTTP_TCP_NO_DELAY,
            NettyHttpServerTransport.SETTING_HTTP_TCP_KEEP_ALIVE,
            NettyHttpServerTransport.SETTING_HTTP_TCP_BLOCKING_SERVER,
            NettyHttpServerTransport.SETTING_HTTP_TCP_REUSE_ADDRESS,
            NettyHttpServerTransport.SETTING_HTTP_TCP_SEND_BUFFER_SIZE,
            NettyHttpServerTransport.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE,
            NettyTransport.WORKER_COUNT,
            NettyTransport.NETTY_MAX_CUMULATION_BUFFER_CAPACITY,
            NettyTransport.NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            NettyTransport.NETTY_RECEIVE_PREDICTOR_SIZE,
            NettyTransport.NETTY_RECEIVE_PREDICTOR_MIN,
            NettyTransport.NETTY_RECEIVE_PREDICTOR_MAX,
            NettyTransport.NETTY_BOSS_COUNT
        );
    }

    public void onModule(NetworkModule networkModule) {
        if (networkModule.canRegisterHttpExtensions()) {
            networkModule.registerHttpTransport("netty", NettyHttpServerTransport.class);
        }
        networkModule.registerTransport("netty", NettyTransport.class);
    }
}
