/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class WatcherPluginTests extends ESTestCase {

    public void testWatcherDisabledTests() throws Exception {
        Settings settings = Settings.builder().put("xpack.watcher.enabled", false).put("path.home", createTempDir()).build();
        Watcher watcher = new Watcher(settings);

        List<ExecutorBuilder<?>> executorBuilders = watcher.getExecutorBuilders(settings);
        assertThat(executorBuilders, hasSize(0));
        assertThat(watcher.createGuiceModules(), hasSize(2));
        assertThat(watcher.getActions(), hasSize(0));
        assertThat(watcher.getRestHandlers(settings, null, null, null, null, null, null), hasSize(0));

        // ensure index module is not called, even if watches index is tried
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(Watch.INDEX, settings);
        AnalysisRegistry registry = new AnalysisRegistry(
            TestEnvironment.newEnvironment(settings),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        IndexModule indexModule = new IndexModule(
            indexSettings,
            registry,
            new InternalEngineFactory(),
            Collections.emptyMap(),
            () -> true,
            TestIndexNameExpressionResolver.newInstance(),
            Collections.emptyMap()
        );
        // this will trip an assertion if the watcher indexing operation listener is null (which it is) but we try to add it
        watcher.onIndexModule(indexModule);

        // also no component creation if not enabled
        assertThat(watcher.createComponents(null, null, null, null, null, null, null, null, null, null, null), hasSize(0));

        watcher.close();
    }

    public void testWatcherUseIlmFallsBackToIlmEnabled() throws Exception {
        boolean ilmEnabled = randomBoolean();
        Settings settingsWithoutWatcherUseIlm = Settings.builder().put(XPackSettings.INDEX_LIFECYCLE_ENABLED.getKey(), ilmEnabled).build();
        assertThat(Watcher.USE_ILM_INDEX_MANAGEMENT.get(settingsWithoutWatcherUseIlm), is(ilmEnabled));

        boolean watcherUseIlmEnabled = randomBoolean();
        Settings settingsWithWatcherUseIlm = Settings.builder()
            .put(XPackSettings.INDEX_LIFECYCLE_ENABLED.getKey(), ilmEnabled)
            .put(Watcher.USE_ILM_INDEX_MANAGEMENT.getKey(), watcherUseIlmEnabled)
            .build();
        assertThat(Watcher.USE_ILM_INDEX_MANAGEMENT.get(settingsWithWatcherUseIlm), is(watcherUseIlmEnabled));

        assertSettingDeprecationsAndWarnings(new Setting<?>[] { XPackSettings.INDEX_LIFECYCLE_ENABLED });
    }

    public void testThreadPoolSize() {
        // old calculation was 5 * number of processors
        assertThat(Watcher.getWatcherThreadPoolSize(true, 1), is(5));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 2), is(10));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 4), is(20));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 8), is(40));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 9), is(45));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 10), is(50));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 16), is(50));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 24), is(50));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 50), is(50));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 51), is(51));
        assertThat(Watcher.getWatcherThreadPoolSize(true, 96), is(96));

        assertThat(Watcher.getWatcherThreadPoolSize(false, scaledRandomIntBetween(1, 100)), is(1));
    }

    public void testReload() {
        Settings settings = Settings.builder().put("xpack.watcher.enabled", true).put("path.home", createTempDir()).build();
        NotificationService<?> mockService = mock(NotificationService.class);
        Watcher watcher = new TestWatcher(settings, mockService);

        watcher.reload(settings);
        verify(mockService, times(1)).reload(settings);
    }

    public void testReloadDisabled() {
        Settings settings = Settings.builder().put("xpack.watcher.enabled", false).put("path.home", createTempDir()).build();
        NotificationService<?> mockService = mock(NotificationService.class);
        Watcher watcher = new TestWatcher(settings, mockService);

        watcher.reload(settings);
        verifyNoMoreInteractions(mockService);
    }

    public void testWatcherSystemIndicesFormat() {
        Settings settings = Settings.builder().put("xpack.watcher.enabled", false).put("path.home", createTempDir()).build();
        Watcher watcher = new Watcher(settings);

        Collection<SystemIndexDescriptor> descriptors = watcher.getSystemIndexDescriptors(settings);
        for (SystemIndexDescriptor descriptor : descriptors) {
            assertThat(descriptor.getIndexFormat(), equalTo(6));
        }
    }

    private class TestWatcher extends Watcher {

        TestWatcher(Settings settings, NotificationService<?> service) {
            super(settings);
            reloadableServices.add(service);
        }
    }
}
