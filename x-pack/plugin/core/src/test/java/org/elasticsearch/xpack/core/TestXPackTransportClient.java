/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.security.SecurityField;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESTestCase.getTestTransportPlugin;

/**
 * TransportClient.Builder that installs the XPackPlugin by default.
 */
@SuppressWarnings({"unchecked","varargs"})
public class TestXPackTransportClient extends TransportClient {

    @SafeVarargs
    public TestXPackTransportClient(Settings settings, Class<? extends Plugin>... plugins) {
        this(settings, Arrays.asList(plugins));
    }

    public TestXPackTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        super(settings, Settings.EMPTY, addPlugins(plugins, getTestTransportPlugin()), null);
    }

    @Override
    public void close() {
        super.close();
        if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings) == false
                || NetworkModule.TRANSPORT_TYPE_SETTING.get(settings).equals(SecurityField.NAME4)) {
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
