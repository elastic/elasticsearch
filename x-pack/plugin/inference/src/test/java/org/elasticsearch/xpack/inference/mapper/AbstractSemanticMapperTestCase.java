/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.diskbbq.DiskBBQPlugin;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.spy;

/**
 * Shared base class for {@link SemanticTextFieldMapperTests} and {@link SemanticFieldMapperTests}.
 * Holds the common setup infrastructure (thread pool, model registry, plugins) and the MapperTestCase
 * overrides that are identical for both semantic field types.
 */
abstract class AbstractSemanticMapperTestCase extends MapperTestCase {

    static class VariableLicenseDiskBBQPlugin extends DiskBBQPlugin {
        private static final Settings STATELESS_SETTINGS = Settings.builder()
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
        static VariableLicenseDiskBBQPlugin BASIC = new VariableLicenseDiskBBQPlugin(
            STATELESS_SETTINGS,
            new XPackLicenseState(() -> 0L, new XPackLicenseStatus(License.OperationMode.BASIC, true, null))
        );
        static VariableLicenseDiskBBQPlugin ENTERPRISE = new VariableLicenseDiskBBQPlugin(
            STATELESS_SETTINGS,
            new XPackLicenseState(() -> 0L, new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null))
        );

        private final XPackLicenseState licenseState;

        VariableLicenseDiskBBQPlugin(Settings settings, XPackLicenseState licenseState) {
            super(settings);
            this.licenseState = requireNonNull(licenseState);
        }

        @Override
        protected XPackLicenseState getLicenseState() {
            return licenseState;
        }
    }

    protected final License.OperationMode operationMode;
    protected ModelRegistry globalModelRegistry;
    private TestThreadPool threadPool;

    AbstractSemanticMapperTestCase(License.OperationMode operationMode) {
        this.operationMode = operationMode;
    }

    @Before
    private void initializeTestEnvironment() {
        threadPool = createThreadPool();
        var clusterService = ClusterServiceUtils.createClusterService(threadPool);
        var modelRegistry = new ModelRegistry(clusterService, new NoOpClient(threadPool), new FeatureService(List.of()));
        globalModelRegistry = spy(modelRegistry);
        globalModelRegistry.clusterChanged(new ClusterChangedEvent("init", clusterService.state(), clusterService.state()) {
            @Override
            public boolean localNodeMaster() {
                return false;
            }
        });
        registerDefaultEndpoints();
    }

    @After
    private void stopThreadPool() {
        threadPool.close();
    }

    /**
     * Called during test setup to register any default inference endpoints needed by the test subclass.
     */
    protected abstract void registerDefaultEndpoints();

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY) {
            @Override
            protected Supplier<ModelRegistry> getModelRegistry() {
                return () -> globalModelRegistry;
            }
        }, new XPackClientPlugin(), switch (operationMode) {
            case ENTERPRISE -> VariableLicenseDiskBBQPlugin.ENTERPRISE;
            case BASIC -> VariableLicenseDiskBBQPlugin.BASIC;
            default -> throw new AssertionError("unknown operation mode: " + operationMode);
        });
    }

    @Override
    protected abstract void minimalMapping(XContentBuilder b) throws IOException;

    @Override
    protected Object getSampleValueForDocument() {
        return null;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // These parameters have complex interdependencies (inference endpoints, model types, dense vs sparse)
        // that cannot be expressed through the simple ParameterChecker mechanism. They are covered by
        // dedicated update tests.
        checker.registerIgnoredParameter("inference_id");
        checker.registerIgnoredParameter("search_inference_id");
        checker.registerIgnoredParameter("model_settings");
        checker.registerIgnoredParameter("chunking_settings");
        checker.registerIgnoredParameter("index_options");
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("doc_values are not supported in semantic fields", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }

    @Override
    protected boolean supportsDocValuesSkippers() {
        return false;
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        // Until a doc is indexed, the query is rewritten as match no docs
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }
}
