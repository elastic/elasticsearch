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

package org.elasticsearch.transport.client;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A builder to create an instance of {@link TransportClient}
 * This class pre-installs the
 * {@link Netty3Plugin},
 * {@link Netty4Plugin},
 * {@link ReindexPlugin},
 * {@link PercolatorPlugin},
 * and {@link MustachePlugin}
 * for the client. These plugins are all elasticsearch core modules required.
 */
@SuppressWarnings({"unchecked","varargs"})
public class PreBuiltTransportClient extends TransportClient {

    private static final Collection<Class<? extends Plugin>> PRE_INSTALLED_PLUGINS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            Netty3Plugin.class,
                            Netty4Plugin.class,
                            TransportPlugin.class,
                            ReindexPlugin.class,
                            PercolatorPlugin.class,
                            MustachePlugin.class));

    @SafeVarargs
    public PreBuiltTransportClient(Settings settings, Class<? extends Plugin>... plugins) {
        this(settings, Arrays.asList(plugins));
    }

    public PreBuiltTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        super(settings, Settings.EMPTY, addPlugins(plugins, PRE_INSTALLED_PLUGINS));
    }

    public static final class TransportPlugin extends Plugin {

        private static final Setting<Boolean> ASSERT_NETTY_BUGLEVEL =
                Setting.boolSetting("netty.assert.buglevel", true, Setting.Property.NodeScope);

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(ASSERT_NETTY_BUGLEVEL);
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                    .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
                    .put("netty.assert.buglevel", true)
                    .build();
        }

    }

}
