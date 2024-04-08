/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.security.Security;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ml.MachineLearning.TRAINED_MODEL_CIRCUIT_BREAKER_NAME;

public class LocalStateMachineLearning extends LocalStateCompositeXPackPlugin {

    private final MachineLearning mlPlugin;

    public LocalStateMachineLearning(final Settings settings, final Path configPath) {
        this(settings, configPath, null);
    }

    protected LocalStateMachineLearning(final Settings settings, final Path configPath, final ExtensionLoader extensionLoader) {
        super(settings, configPath);
        LocalStateMachineLearning thisVar = this;
        mlPlugin = new MachineLearning(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        };
        mlPlugin.loadExtensions(extensionLoader);
        mlPlugin.setCircuitBreaker(new NoopCircuitBreaker(TRAINED_MODEL_CIRCUIT_BREAKER_NAME));
        plugins.add(mlPlugin);
        plugins.add(new Monitoring(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected LicenseService getLicenseService() {
                return thisVar.getLicenseService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
        plugins.add(new Security(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        });
        plugins.add(new MockedRollupPlugin());
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return mlPlugin.getAggregations();
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return mlPlugin.getCharFilters();
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return mlPlugin.getTokenizers();
    }

    /**
     * This is only required as we now have to have the GetRollupIndexCapsAction as a valid action in our node.
     * The MachineLearningLicenseTests attempt to create a datafeed referencing this LocalStateMachineLearning object.
     * Consequently, we need to be able to take this rollup action (response does not matter)
     * as the datafeed extractor now depends on it.
     */
    public static class MockedRollupPlugin extends Plugin implements ActionPlugin {

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Collections.singletonList(new ActionHandler<>(GetRollupIndexCapsAction.INSTANCE, MockedRollupIndexCapsTransport.class));
        }

        public static class MockedRollupIndexCapsTransport extends TransportAction<
            GetRollupIndexCapsAction.Request,
            GetRollupIndexCapsAction.Response> {

            @Inject
            public MockedRollupIndexCapsTransport(TransportService transportService) {
                super(GetRollupIndexCapsAction.NAME, new ActionFilters(new HashSet<>()), transportService.getTaskManager());
            }

            @Override
            protected void doExecute(
                Task task,
                GetRollupIndexCapsAction.Request request,
                ActionListener<GetRollupIndexCapsAction.Response> listener
            ) {
                listener.onResponse(new GetRollupIndexCapsAction.Response());
            }
        }
    }
}
