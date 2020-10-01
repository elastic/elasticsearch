/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.collect.List;
import org.elasticsearch.common.settings.Settings;
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
    protected Settings nodeSettings(final int nodeOrdinal) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        builder.put(Autoscaling.AUTOSCALING_ENABLED_SETTING.getKey(), true);
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
        builder.put(Autoscaling.AUTOSCALING_ENABLED_SETTING.getKey(), true);
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        return builder.build();
    }

}
