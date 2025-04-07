/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestOnlyMultiProjectResolverFactoryTests extends ESTestCase {

    public void testMultiProjectEnabled() {
        final Settings settings = Settings.builder().put("test.multi_project.enabled", true).build();
        var plugin = new TestOnlyMultiProjectPlugin(settings);
        var pluginServices = mock(Plugin.PluginServices.class);
        when(pluginServices.threadPool()).thenReturn(mock(ThreadPool.class));
        plugin.createComponents(pluginServices);

        assertThat(new TestOnlyMultiProjectResolverFactory(plugin).create(), instanceOf(TestOnlyMultiProjectResolver.class));
    }

    public void testMultiProjectDisabled() {
        final Settings settings = randomBoolean() ? Settings.EMPTY : Settings.builder().put("test.multi_project.enabled", false).build();
        var plugin = new TestOnlyMultiProjectPlugin(settings);
        var pluginServices = mock(Plugin.PluginServices.class);
        when(pluginServices.threadPool()).thenReturn(mock(ThreadPool.class));
        plugin.createComponents(pluginServices);

        assertThat(new TestOnlyMultiProjectResolverFactory(plugin).create(), sameInstance(DefaultProjectResolver.INSTANCE));
    }

}
