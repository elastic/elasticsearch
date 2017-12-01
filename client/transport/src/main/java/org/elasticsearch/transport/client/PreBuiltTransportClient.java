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

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * A builder to create an instance of {@link TransportClient}. This class pre-installs the
 * {@link Netty4Plugin},
 * {@link ReindexPlugin},
 * {@link PercolatorPlugin},
 * {@link MustachePlugin},
 * {@link ParentJoinPlugin}
 * plugins for the client. These plugins are all the required modules for Elasticsearch.
 *
 * Note that {@link TransportClient} will be deprecated in Elasticsearch 7.0 and removed in Elasticsearch 8.0.
 * Use the High Level REST Client instead.
 */
@SuppressWarnings({"unchecked","varargs"})
public class PreBuiltTransportClient extends TransportClient {

    static {
        // initialize Netty system properties before triggering any Netty class loads
        initializeNetty();
    }

    /**
     * Netty wants to do some unwelcome things like use unsafe and replace a private field, or use a poorly considered buffer recycler. This
     * method disables these things by default, but can be overridden by setting the corresponding system properties.
     */
    private static void initializeNetty() {
        /*
         * We disable three pieces of Netty functionality here:
         *  - we disable Netty from being unsafe
         *  - we disable Netty from replacing the selector key set
         *  - we disable Netty from using the recycler
         *
         * While permissions are needed to read and set these, the permissions needed here are innocuous and thus should simply be granted
         * rather than us handling a security exception here.
         */
        setSystemPropertyIfUnset("io.netty.noUnsafe", Boolean.toString(true));
        setSystemPropertyIfUnset("io.netty.noKeySetOptimization", Boolean.toString(true));
        setSystemPropertyIfUnset("io.netty.recycler.maxCapacityPerThread", Integer.toString(0));
    }

    @SuppressForbidden(reason = "set system properties to configure Netty")
    private static void setSystemPropertyIfUnset(final String key, final String value) {
        final String currentValue = System.getProperty(key);
        if (currentValue == null) {
            System.setProperty(key, value);
        }
    }

    private static final Collection<Class<? extends Plugin>> PRE_INSTALLED_PLUGINS =
        Collections.unmodifiableList(
            Arrays.asList(
                Netty4Plugin.class,
                ReindexPlugin.class,
                PercolatorPlugin.class,
                MustachePlugin.class,
                ParentJoinPlugin.class));

    /**
     * Creates a new transport client with pre-installed plugins.
     *
     * @param settings the settings passed to this transport client
     * @param plugins  an optional array of additional plugins to run with this client
     */
    @SafeVarargs
    public PreBuiltTransportClient(Settings settings, Class<? extends Plugin>... plugins) {
        this(settings, Arrays.asList(plugins));
    }

    /**
     * Creates a new transport client with pre-installed plugins.
     *
     * @param settings the settings passed to this transport client
     * @param plugins  a collection of additional plugins to run with this client
     */
    public PreBuiltTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        this(settings, plugins, null);
    }

    /**
     * Creates a new transport client with pre-installed plugins.
     *
     * @param settings            the settings passed to this transport client
     * @param plugins             a collection of additional plugins to run with this client
     * @param hostFailureListener a failure listener that is invoked if a node is disconnected; this can be <code>null</code>
     */
    public PreBuiltTransportClient(
        Settings settings,
        Collection<Class<? extends Plugin>> plugins,
        HostFailureListener hostFailureListener) {
        super(settings, Settings.EMPTY, addPlugins(plugins, PRE_INSTALLED_PLUGINS), hostFailureListener);
    }

    @Override
    public void close() {
        super.close();
        if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings) == false
            || NetworkModule.TRANSPORT_TYPE_SETTING.get(settings).equals(Netty4Plugin.NETTY_TRANSPORT_NAME)) {
            try {
                GlobalEventExecutor.INSTANCE.awaitInactivity(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                ThreadDeathWatcher.awaitInactivity(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
