/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.core.XPackSettings.ML_NATIVE_CODE_PLATFORMS;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.mockito.Mockito.mock;

public class InferencePluginTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() throws Exception {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void tearDownThreadPool() throws Exception {
        terminate(threadPool);
    }

    /**
     * Constructs a factory context, instantiates each factory returned by
     * {@link InferencePlugin#getInferenceServiceFactories()}, and collects the service names.
     * <p>
     * External HTTP service factories reference {@code httpFactory.get()} and
     * {@code serviceComponents.get()}, which are uninitialised without a call to
     * {@link InferencePlugin#createComponents}. Those factories throw a
     * {@link NullPointerException} at construction time and are intentionally skipped here —
     * only the {@link ElasticsearchInternalService}, which has no such dependency, will be
     * successfully constructed.
     */
    private Set<String> serviceNamesFromFactories(Settings settings) throws IOException {
        var factoryContext = new InferenceServiceExtension.InferenceServiceFactoryContext(
            mock(Client.class),
            threadPool,
            mockClusterServiceEmpty(),
            settings,
            mock(InferenceStats.class),
            mock(FeatureService.class)
        );
        var names = new HashSet<String>();
        try (var plugin = new InferencePlugin(settings)) {
            for (var factory : plugin.getInferenceServiceFactories()) {
                try (var service = factory.create(factoryContext)) {
                    names.add(service.name());
                } catch (NullPointerException ignored) {
                    // Expected for external HTTP services whose factories are uninitialised
                }
            }
        }
        return names;
    }

    public void testGetInferenceServiceFactories_includesElasticsearchServiceByDefault() throws IOException {
        assumeTrue("ML native code required", ML_NATIVE_CODE_PLATFORMS.contains(Platforms.PLATFORM_NAME));
        assertTrue(serviceNamesFromFactories(Settings.EMPTY).contains(ElasticsearchInternalService.NAME));
    }

    public void testGetInferenceServiceFactories_excludesElasticsearchServiceWhenNlpDisabled() throws IOException {
        var settings = Settings.builder().put("xpack.ml.nlp.enabled", false).build();
        assertFalse(serviceNamesFromFactories(settings).contains(ElasticsearchInternalService.NAME));
    }

    public void testGetInferenceServiceFactories_excludesElasticsearchServiceWhenMlDisabled() throws IOException {
        var settings = Settings.builder().put("xpack.ml.enabled", false).build();
        assertFalse(serviceNamesFromFactories(settings).contains(ElasticsearchInternalService.NAME));
    }
}
