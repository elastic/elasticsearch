/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.client;

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.SecurityField;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * A builder to create an instance of {@link TransportClient} that pre-installs
 * all of the plugins installed by the {@link PreBuiltTransportClient} and the
 * {@link XPackPlugin} so that the client may be used with an x-pack enabled
 * cluster.
 *
 * Note that {@link TransportClient} will be deprecated in Elasticsearch 7.0 and removed in Elasticsearch 8.0.
 * Use the High Level REST Client instead. Support for x-pack API is going to be added.
 */
@SuppressWarnings({"unchecked","varargs"})
public class PreBuiltXPackTransportClient extends PreBuiltTransportClient {

    @SafeVarargs
    public PreBuiltXPackTransportClient(Settings settings, Class<? extends Plugin>... plugins) {
        this(settings, Arrays.asList(plugins));
    }

    public PreBuiltXPackTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        this(settings, plugins, null);
    }

    public PreBuiltXPackTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins,
                                        HostFailureListener hostFailureListener) {
        super(settings, addPlugins(plugins, Collections.singletonList(XPackClientPlugin.class)), hostFailureListener);
    }

    @Override
    public void close() {
        super.close();
        if (NetworkModule.TRANSPORT_TYPE_SETTING.get(settings).equals(SecurityField.NAME4)) {
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
