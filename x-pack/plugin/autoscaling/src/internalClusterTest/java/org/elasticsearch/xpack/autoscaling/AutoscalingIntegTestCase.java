/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.List;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collection;

public abstract class AutoscalingIntegTestCase extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateAutoscaling.class);
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return List.of(LocalStateAutoscaling.class, getTestTransportPlugin());
    }

    @Override
    protected Settings transportClientSettings() {
        final Settings.Builder builder = Settings.builder().put(super.transportClientSettings());
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        return builder.build();
    }

}
