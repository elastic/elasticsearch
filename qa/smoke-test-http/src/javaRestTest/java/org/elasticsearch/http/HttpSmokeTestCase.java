/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.nio.MockNioTransportPlugin;
import org.elasticsearch.transport.nio.NioTransportPlugin;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;

public abstract class HttpSmokeTestCase extends ESIntegTestCase {

    private static String nodeTransportTypeKey;
    private static String nodeHttpTypeKey;
    private static String clientTypeKey;

    @SuppressWarnings("unchecked")
    @BeforeClass
    public static void setUpTransport() {
        nodeTransportTypeKey = getTypeKey(randomFrom(getTestTransportPlugin(), Netty4Plugin.class, NioTransportPlugin.class));
        nodeHttpTypeKey = getHttpTypeKey(randomFrom(Netty4Plugin.class, NioTransportPlugin.class));
        clientTypeKey = getTypeKey(randomFrom(getTestTransportPlugin(), Netty4Plugin.class, NioTransportPlugin.class));
    }

    private static String getTypeKey(Class<? extends Plugin> clazz) {
        if (clazz.equals(MockNioTransportPlugin.class)) {
            return MockNioTransportPlugin.MOCK_NIO_TRANSPORT_NAME;
        } else if (clazz.equals(NioTransportPlugin.class)) {
            return NioTransportPlugin.NIO_TRANSPORT_NAME;
        } else {
            assert clazz.equals(Netty4Plugin.class);
            return Netty4Plugin.NETTY_TRANSPORT_NAME;
        }
    }

    private static String getHttpTypeKey(Class<? extends Plugin> clazz) {
        if (clazz.equals(NioTransportPlugin.class)) {
            return NioTransportPlugin.NIO_HTTP_TRANSPORT_NAME;
        } else {
            assert clazz.equals(Netty4Plugin.class);
            return Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME;
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.TRANSPORT_TYPE_KEY, nodeTransportTypeKey)
            .put(NetworkModule.HTTP_TYPE_KEY, nodeHttpTypeKey)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(getTestTransportPlugin(), Netty4Plugin.class, NioTransportPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(getTestTransportPlugin(), Netty4Plugin.class, NioTransportPlugin.class);
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder().put(super.transportClientSettings()).put(NetworkModule.TRANSPORT_TYPE_KEY, clientTypeKey).build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }
}
