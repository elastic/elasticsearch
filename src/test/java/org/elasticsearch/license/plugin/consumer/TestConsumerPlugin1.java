/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class TestConsumerPlugin1 extends TestConsumerPluginBase {

    public final static String NAME = "test_consumer_plugin_1";

    @Inject
    public TestConsumerPlugin1(Settings settings) {
        super(settings);
    }

    @Override
    protected Class<? extends LifecycleComponent> service() {
        return TestPluginService1.class;
    }

    @Override
    protected String pluginName() {
        return NAME;
    }
}
