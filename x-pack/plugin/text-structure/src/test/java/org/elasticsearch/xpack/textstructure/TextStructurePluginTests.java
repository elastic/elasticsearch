/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.textstructure.transport.TextStructExecutor;

import java.util.Collection;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TextStructurePluginTests extends ESTestCase {

    public void testCreateComponentsShouldPublishSingleSharedExecutor() {
        Settings settings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 2.0).build();
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(settings);
        Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.environment()).thenReturn(environment);

        try (TestThreadPool threadPool = new TestThreadPool(getTestName(), settings)) {
            when(services.threadPool()).thenReturn(threadPool);
            Collection<?> components = new TextStructurePlugin().createComponents(services);
            assertThat(components, contains(instanceOf(TextStructExecutor.class)));
        }
    }
}
