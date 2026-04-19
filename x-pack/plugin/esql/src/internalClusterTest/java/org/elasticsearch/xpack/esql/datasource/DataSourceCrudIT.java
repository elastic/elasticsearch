/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

/**
 * Integration coverage for the ES|QL data-source + dataset CRUD API shipped by
 * {@code DataSourceService} / {@code DatasetService} (PR #146600 / esql-planning#453).
 *
 * <p>Covers scenarios that the unit-test harness ({@code InMemoryDataSourceService} /
 * {@code InMemoryDatasetService}, which inline task {@code execute()} synchronously) and the
 * YAML REST tests (single-node, no loaded storage-plugin validators) cannot reach:
 *
 * <ol>
 *   <li><b>Full lifecycle</b> — PUT data source → PUT dataset → GET from non-master → DELETE dataset → DELETE data source.</li>
 *   <li><b>Cluster-state propagation</b> — PUT via master client, GET via non-master client, same content.</li>
 *   <li><b>Gateway persistence</b> — PUT data source, {@code internalCluster().fullRestart()}, data source survives.
 *       Covers {@code DataSourceMetadata.context() = EnumSet.of(GATEWAY)}.</li>
 *   <li><b>Concurrent PUT same data source</b> — two clients via {@code CountDownLatch}, last-write-wins semantics,
 *       cluster-state consistency held by {@code MasterService}'s single-threaded task queue.</li>
 *   <li><b>DELETE data source racing dataset PUT</b> — exactly one of two valid outcomes:
 *       (a) DELETE wins → dataset PUT re-check throws {@code ResourceNotFoundException};
 *       (b) PUT wins → DELETE sees dependent dataset, throws {@code ElasticsearchStatusException(CONFLICT)}.</li>
 *   <li><b>Dispatch-vs-task-execute window for dataset PUT vs parent DELETE</b> — uses the {@code CyclicBarrier(2)}
 *       + blocking-master-task pattern from {@code MetadataCreateIndexServiceIT.java:124-138} to force the race:
 *       dataset PUT captures parent at dispatch time, parent gets deleted before the CAS task fires, task-level
 *       re-check throws {@code ResourceNotFoundException}. The one scenario that definitively justifies this IT
 *       over the in-memory unit-test harness.</li>
 *   <li><b>Index creation collides with existing dataset</b> — {@code MetadataCreateIndexService.validateIndexName}
 *       pre-task check calls {@code ProjectMetadata.hasDataset()}; rejects with
 *       {@code InvalidIndexNameException("... already exists as an ESQL dataset")}.</li>
 * </ol>
 *
 * <p>Companion unit test for scenario 6: {@code DatasetServiceDispatchVsExecuteTests} in {@code src/test/}
 * fabricates two {@code ClusterState} snapshots (dispatch-time has parent, execute-time doesn't) and drives
 * the task body directly. Pins correctness cheaply; this IT proves end-to-end.
 *
 * <p><b>Feature flag</b>: enabled via {@code es.esql_external_datasources_feature_flag_enabled=true} system
 * property set in {@link #enableFeatureFlag()} before node startup. {@code FeatureFlag} reads the property
 * once per JVM at construction; {@code @BeforeClass} is the right hook.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = true, minNumDataNodes = 2)
public class DataSourceCrudIT extends ESIntegTestCase {

    /**
     * Enable the external-datasources feature flag before node startup. Must be a {@code @BeforeClass}
     * hook, not an instance-level or {@code nodeSettings()} override: {@code FeatureFlag.isEnabled()}
     * reads the system property once at {@code DataSourceMetadata} class load, which happens as nodes
     * start.
     */
    @BeforeClass
    public static void enableFeatureFlag() {
        System.setProperty("es.esql_external_datasources_feature_flag_enabled", "true");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDataSource.class);
    }

    // ---------------------------------------------------------------------------------------------
    // Test scenarios — stubbed; bodies to be filled in.
    // ---------------------------------------------------------------------------------------------

    public void testFullLifecycle() {
        // TODO: PUT data source → PUT dataset → GET from non-master client → DELETE dataset → DELETE data source.
        // Precedent: server/src/internalClusterTest/java/org/elasticsearch/indices/template/ComposableTemplateIT.java
    }

    public void testClusterStatePropagation() {
        // TODO: PUT via internalCluster().masterClient(), GET via internalCluster().nonMasterClient(), assert parity.
        // Precedent: server/src/internalClusterTest/java/org/elasticsearch/cluster/SimpleClusterStateIT.java
    }

    public void testGatewayPersistence() throws Exception {
        // TODO: PUT data source, internalCluster().fullRestart(), ensureYellow(), assert data source still readable.
        // Covers DataSourceMetadata.context() = EnumSet.of(GATEWAY) at DataSourceMetadata.java:117.
        // Precedent: server/src/internalClusterTest/java/org/elasticsearch/snapshots/CustomMetadataContextIT.java:120-135
    }

    public void testConcurrentPutSameDataSource() throws Exception {
        // TODO: Two threads via CountDownLatch submit PUT requests for the same data-source name.
        // MasterService's single-threaded task executor serializes — assert one winner, assert state
        // is consistent (either value A or value B, never mixed). Do NOT assert which winner — flaky.
        // Precedent: server/src/internalClusterTest/java/org/elasticsearch/versioning/ConcurrentSeqNoVersioningIT.java
    }

    public void testDeleteDataSourceRacingDatasetPut() throws Exception {
        // TODO: Latch fan-out: submit PUT dataset referencing parent + DELETE parent data source concurrently.
        // Two valid outcomes (assert end state is exactly one):
        // a) DELETE wins → dataset PUT CAS task throws ResourceNotFoundException (~DatasetService.java:142)
        // b) PUT wins → DELETE sees dependent dataset, throws ElasticsearchStatusException(CONFLICT)
        // at ~DataSourceService.java:184
        // Precedent: server/src/internalClusterTest/java/org/elasticsearch/cluster/metadata/MetadataCreateIndexServiceIT.java:90-180
    }

    public void testDispatchVsTaskExecuteRace() throws Exception {
        // TODO: THE scenario that justifies this IT. Pattern from MetadataCreateIndexServiceIT.java:124-138:
        // 1. Submit a blocking cluster-state task via masterClusterService().submitUnbatchedStateUpdateTask("block", ...)
        // that safeAwait()s a CyclicBarrier(2).
        // 2. While master is blocked:
        // - Fire dataset PUT. Pre-task validation in DatasetService.putDataset reads
        // ClusterService.state() (not the blocked state) and sees parent exists.
        // Task submitted to master queue behind the blocker.
        // - Fire parent data-source DELETE. Task also submitted to master queue.
        // 3. Release the barrier. Master drains in submission order.
        // 4. Assert: dataset PUT listener receives ResourceNotFoundException (the task-level re-check
        // at ~DatasetService.java:142 finds the parent gone).
        //
        // Why this one justifies the IT: the in-memory unit tests inline execute() synchronously, so
        // the dispatch-vs-execute window collapses. Only a real MasterServiceTaskQueue exercise reaches
        // this code path.
    }

    public void testIndexCreationCollidesWithDataset() throws Exception {
        // TODO: PUT dataset "my_collision", then client().admin().indices().prepareCreate("my_collision").get().
        // Assert InvalidIndexNameException with message containing "already exists as an ESQL dataset".
        // Rejection happens pre-task in MetadataCreateIndexService.validateIndexName:314 via
        // ProjectMetadata.hasDataset() (ProjectMetadata.java:566). CAS-time backstop is
        // ProjectMetadata.Builder.ensureNoNameCollisions at ProjectMetadata.java:1775.
    }

    // ---------------------------------------------------------------------------------------------
    // Test infrastructure — inner classes.
    // ---------------------------------------------------------------------------------------------

    /**
     * Test-only composite plugin. Subclasses {@link LocalStateCompositeXPackPlugin} to bring xpack
     * wiring into a multi-node ESIntegTestCase, adds {@link EsqlPlugin} with {@code loadExtensions}
     * no-op'd (suppresses production SPI data-source discovery), and adds {@link TestDataSourcePlugin}
     * so exactly one validator ({@code type="test"}) is registered — deterministic, closed-world.
     * Matches the pattern in {@code LocalStateView} at {@code x-pack/plugin/esql/src/test/.../view/}.
     */
    public static class LocalStateDataSource extends LocalStateCompositeXPackPlugin {

        public LocalStateDataSource(final Settings settings, final Path configPath) throws Exception {
            super(settings, configPath);

            plugins.add(new EsqlPlugin() {
                @Override
                protected XPackLicenseState getLicenseState() {
                    return LocalStateDataSource.this.getLicenseState();
                }

                @Override
                public void loadExtensions(ExtensionLoader loader) {
                    // No-op — we want a closed-world test with only the validators registered below,
                    // not whatever DataSourcePlugin impls happen to be on the classpath.
                }
            });

            plugins.add(new TestDataSourcePlugin());
        }
    }

    /**
     * Test-only plugin that exports a single {@link TestValidator} for {@code type="test"}. Must
     * extend {@link Plugin} (to go into {@code LocalStateCompositeXPackPlugin.plugins}) and
     * implement {@link DataSourcePlugin} (to register a validator with {@link EsqlPlugin}) —
     * exactly the same pair every real storage plugin declares (see {@code S3DataSourcePlugin}).
     */
    public static class TestDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
            return Map.of("test", new TestValidator());
        }
    }

    /**
     * Minimal pass-through validator for IT scenarios. Accepts any map, marks keys starting with
     * {@code secret_} as secrets. Mirrors {@code TestDataSourceValidator} in {@code src/test/}
     * but inlined here to avoid cross-source-set dependency. Tests that need error-path behaviour
     * (throw on validate) can subclass and override.
     */
    static class TestValidator implements DataSourceValidator {
        @Override
        public String type() {
            return "test";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            Map<String, DataSourceSetting> out = new HashMap<>();
            for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
                boolean secret = e.getKey().startsWith("secret_");
                out.put(e.getKey(), new DataSourceSetting(e.getValue(), secret));
            }
            return out;
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            return new HashMap<>(datasetSettings);
        }
    }

    @SuppressWarnings("unused")
    private static final Class<?> VALIDATION_EXCEPTION_REF = ValidationException.class;

    // Keep the DataSourceMetadata import alive for IDE/reader navigation from javadoc.
    @SuppressWarnings("unused")
    private static final Class<?> METADATA_REF = DataSourceMetadata.class;
}
