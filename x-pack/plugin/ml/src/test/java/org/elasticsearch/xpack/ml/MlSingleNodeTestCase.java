/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.MockDeterministicScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.MlDataFrameAnalysisNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.ml.aggs.correlation.CorrelationNamedContentProvider;
import org.elasticsearch.xpack.ml.inference.modelsize.MlModelSizeNamedXContentProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * An extension to {@link ESSingleNodeTestCase} that adds node settings specifically needed for ML test cases.
 */
public abstract class MlSingleNodeTestCase extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());

        // Disable native ML autodetect_process as the c++ controller won't be available
        newSettings.put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false);
        newSettings.put(MachineLearningField.MAX_MODEL_MEMORY_LIMIT.getKey(), ByteSizeValue.ofBytes(1024));
        newSettings.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        // Disable security otherwise delete-by-query action fails to get authorized
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        // Disable ILM history index so that the tests don't have to clean it up
        newSettings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        return newSettings.build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlModelSizeNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new CorrelationNamedContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(
            LocalStateMachineLearning.class,
            DataStreamsPlugin.class,
            ReindexPlugin.class,
            IngestCommonPlugin.class,
            MockPainlessScriptEngine.TestPlugin.class,
            // ILM is required for .ml-state template index settings
            IndexLifecycle.class,
            // Needed for scaled_float
            MapperExtrasPlugin.class
        );
    }

    @Override
    public void tearDown() throws Exception {
        try {
            logger.trace("[{}#{}]: ML-specific after test cleanup", getTestClass().getSimpleName(), getTestName());
            client().execute(ResetFeatureStateAction.INSTANCE, new ResetFeatureStateRequest()).actionGet();
        } finally {
            super.tearDown();
        }
    }

    protected void waitForMlTemplates() throws Exception {
        // block until the templates are installed
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue("Timed out waiting for the ML templates to be installed", MachineLearning.criticalTemplatesInstalled(state));
        });
    }

    protected <T> void blockingCall(Consumer<ActionListener<T>> function, AtomicReference<T> response, AtomicReference<Exception> error)
        throws InterruptedException {
        blockingCall(function, response::set, error::set);
    }

    protected <T> void blockingCall(Consumer<ActionListener<T>> function, Consumer<T> response, Consumer<Exception> error)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> {
            response.accept(r);
            latch.countDown();
        }, e -> {
            error.accept(e);
            latch.countDown();
        });

        function.accept(listener);
        latch.await();
    }

    protected <T> T blockingCall(Consumer<ActionListener<T>> function) throws Exception {
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<T> responseHolder = new AtomicReference<>();
        blockingCall(function, responseHolder, exceptionHolder);
        if (exceptionHolder.get() != null) {
            assertNull(exceptionHolder.get().getMessage(), exceptionHolder.get());
        }
        return responseHolder.get();
    }

    protected static ThreadPool mockThreadPool() {
        ThreadPool tp = mock(ThreadPool.class);
        ExecutorService executor = mock(ExecutorService.class);
        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[0]).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        when(tp.executor(any(String.class))).thenReturn(executor);
        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[0]).run();
            return null;
        }).when(tp).schedule(any(Runnable.class), any(TimeValue.class), any(String.class));
        return tp;
    }

    public static void assertNoException(AtomicReference<Exception> error) throws Exception {
        if (error.get() == null) {
            return;
        }
        throw error.get();
    }

    public static class MockPainlessScriptEngine extends MockScriptEngine {

        public static final String NAME = "painless";

        public static class TestPlugin extends MockScriptPlugin {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new MockPainlessScriptEngine();
            }

            @Override
            protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
                return Collections.emptyMap();
            }
        }

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public <T> T compile(String name, String script, ScriptContext<T> context, Map<String, String> options) {
            if (context.instanceClazz.equals(ScoreScript.class)) {
                return context.factoryClazz.cast(new MockScoreScript(MockDeterministicScript.asDeterministic(p -> 0.0)));
            }
            if (context.name.equals("ingest")) {
                IngestScript.Factory factory = vars -> new IngestScript(vars) {
                    @Override
                    public void execute(Map<String, Object> ctx) {}
                };
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("mock painless does not know how to handle context [" + context.name + "]");
        }
    }
}
