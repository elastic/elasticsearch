/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class TestConsumerPlugin2 extends TestConsumerPluginBase {

    public static String NAME = "test_consumer_plugin_2";

    @Inject
    public TestConsumerPlugin2(Settings settings) {
        super(settings);
    }

    @Override
    protected Class<? extends LifecycleComponent> service() {
        return TestPluginService2.class;
    }

    @Override
    protected String pluginName() {
        return NAME;
    }
}
