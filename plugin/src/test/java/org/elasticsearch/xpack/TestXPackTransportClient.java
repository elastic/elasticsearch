/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.Collection;

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
        super(settings, Settings.EMPTY, addPlugins(plugins, XPackPlugin.class, getTestTransportPlugin()), null);
    }
}
