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

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty3.Netty3HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.netty3.Netty3Transport;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;

public class Netty3Plugin extends Plugin {
    public static final String NETTY_TRANSPORT_NAME = "netty3";
    public static final String NETTY_HTTP_TRANSPORT_NAME = "netty3";

    public Netty3Plugin(Settings settings) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                Class.forName("org.jboss.netty.channel.socket.nio.SelectorUtil");
            } catch (ClassNotFoundException e) {
                throw new AssertionError(e); // we don't do anything with this
            }
            return null;
        });
        /*
         * Asserts that sun.nio.ch.bugLevel has been set to a non-null value. This assertion will fail if the corresponding code
         * is not executed in a doPrivileged block. This can be disabled via `netty.assert.buglevel` setting which isn't registered
         * by default but test can do so if they depend on the jar instead of the module.
         */
        //TODO Once we have no jar level dependency we can get rid of this.
        if (settings.getAsBoolean("netty.assert.buglevel", true)) {
            assert System.getProperty("sun.nio.ch.bugLevel") != null :
                "sun.nio.ch.bugLevel is null somebody pulls in SelectorUtil without doing stuff in a doPrivileged block?";
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            Netty3HttpServerTransport.SETTING_HTTP_NETTY_MAX_CUMULATION_BUFFER_CAPACITY,
            Netty3HttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty3HttpServerTransport.SETTING_HTTP_WORKER_COUNT,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_NO_DELAY,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_KEEP_ALIVE,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_BLOCKING_SERVER,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_REUSE_ADDRESS,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_SEND_BUFFER_SIZE,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE,
            Netty3Transport.WORKER_COUNT,
            Netty3Transport.NETTY_MAX_CUMULATION_BUFFER_CAPACITY,
            Netty3Transport.NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty3Transport.NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty3Transport.NETTY_RECEIVE_PREDICTOR_MIN,
            Netty3Transport.NETTY_RECEIVE_PREDICTOR_MAX,
            Netty3Transport.NETTY_BOSS_COUNT
        );
    }

    public void onModule(NetworkModule networkModule) {
        if (networkModule.canRegisterHttpExtensions()) {
            networkModule.registerHttpTransport(NETTY_HTTP_TRANSPORT_NAME, Netty3HttpServerTransport.class);
        }
        networkModule.registerTransport(NETTY_TRANSPORT_NAME, Netty3Transport.class);
    }
}
