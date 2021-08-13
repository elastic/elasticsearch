/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.nio.NioTransportPlugin;

import java.util.Collection;
import java.util.Collections;

public abstract class NioIntegTestCase extends ESIntegTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected boolean addMockTransportService() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // randomize nio settings
        if (randomBoolean()) {
            builder.put(NioTransportPlugin.NIO_WORKER_COUNT.getKey(), random().nextInt(3) + 1);
            builder.put(NioTransportPlugin.NIO_HTTP_WORKER_COUNT.getKey(), random().nextInt(3) + 1);
        }
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, NioTransportPlugin.NIO_TRANSPORT_NAME);
        builder.put(NetworkModule.HTTP_TYPE_KEY, NioTransportPlugin.NIO_HTTP_TRANSPORT_NAME);
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(NioTransportPlugin.class);
    }
}
