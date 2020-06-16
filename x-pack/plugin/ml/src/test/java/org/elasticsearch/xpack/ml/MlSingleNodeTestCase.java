/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

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
        newSettings.put(MachineLearningField.MAX_MODEL_MEMORY_LIMIT.getKey(), new ByteSizeValue(1024));
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
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(
            LocalStateMachineLearning.class,
            // ILM is required for .ml-state template index settings
            IndexLifecycle.class);
    }

    /**
     * This cleanup is to fix the problem described in
     * https://github.com/elastic/elasticsearch/issues/38952
     */
    @Override
    public void tearDown() throws Exception {
        try {
            logger.trace("[{}#{}]: ML-specific after test cleanup", getTestClass().getSimpleName(), getTestName());
            String[] nonAnnotationMlIndices;
            boolean mlAnnotationsIndexExists;
            do {
                String[] mlIndices = client().admin().indices().prepareGetIndex().addIndices(".ml-*").get().indices();
                nonAnnotationMlIndices = Arrays.stream(mlIndices).filter(name -> name.startsWith(".ml-annotations") == false)
                    .toArray(String[]::new);
                mlAnnotationsIndexExists = mlIndices.length > nonAnnotationMlIndices.length;
            } while (nonAnnotationMlIndices.length > 0 && mlAnnotationsIndexExists == false);
            if (nonAnnotationMlIndices.length > 0) {
                // Delete the ML indices apart from the annotations index.  The annotations index will be deleted by the
                // base class cleanup.  We want to delete all the others first so that the annotations index doesn't get
                // automatically recreated.
                assertAcked(client().admin().indices().prepareDelete(nonAnnotationMlIndices).get());
            }
        } finally {
            super.tearDown();
        }
    }

    protected void waitForMlTemplates() throws Exception {
        // block until the templates are installed
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue("Timed out waiting for the ML templates to be installed",
                    MachineLearning.allTemplatesInstalled(state));
        });
    }

    protected <T> void blockingCall(Consumer<ActionListener<T>> function, AtomicReference<T> response,
                                  AtomicReference<Exception> error) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(
                r -> {
                    response.set(r);
                    latch.countDown();
                },
                e -> {
                    error.set(e);
                    latch.countDown();
                }
        );

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

}
