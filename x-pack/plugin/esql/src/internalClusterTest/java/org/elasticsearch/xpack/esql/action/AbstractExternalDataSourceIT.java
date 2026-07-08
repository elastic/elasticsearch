/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.ObjIntConsumer;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Shared base for {@code FROM <dataset>} external-datasource integration tests.
 *
 * <p>Each subclass registers a {@code test}-type data source plus one or more datasets (each pointing at a
 * local or remote resource), then queries them with {@code FROM <name>}. The dataset's format reader is
 * selected by the resource's file extension — the dataset model exposes no {@code reader}/{@code format}
 * selector — so registering with no {@code format} setting keeps the same extension-driven reader and
 * compression-codec resolution the legacy {@code EXTERNAL} command used.
 *
 * <p>Centralizes the boilerplate that every such test used to copy by hand:
 * <ul>
 *   <li>the {@link EsqlEnterpriseWithDatasourceExtensions} plugin that re-enables SPI extension
 *       loading (so the data-source plugins added in {@link #formatPlugins()} are discovered);</li>
 *   <li>a pass-through {@link TestDataSourcePlugin} (type {@code test}) so dataset/data-source
 *       registration accepts arbitrary resource schemes (e.g. {@code file://}) in tests;</li>
 *   <li>{@link #registerDataSource}/{@link #registerDataset} helpers that record what they create
 *       and tear it down in {@link #cleanupRegistry()} (best-effort), so tests do not hand-roll the
 *       PUT/DELETE request plumbing or the {@code @After} cleanup;</li>
 *   <li>shared fixture writers ({@link #createOutputFile}, {@link #writeParquet}, {@link #writeGzipped}).</li>
 * </ul>
 *
 * <p>The wiring mirrors the known-good configuration of {@code FromDatasetIT} /
 * {@code AbstractExternalMetadataMatrixIT}: extensions are re-enabled and {@code TestDataSourcePlugin} is
 * supplied as a node plugin (a single discovery path), so {@code EsqlPlugin}'s duplicate-validator guard
 * never trips.
 *
 * <p>A single {@link #requireFeatureFlag()} {@code @Before} gates every subclass on the
 * {@code dataset-in-from-command} capability (which also gates {@code FROM <dataset>} resolution) and the
 * local-filesystem feature flag, and {@link #nodeSettings} allowlists the shared temp-dir root for
 * {@code file://} access, so subclasses do not repeat either the assume or the settings override.
 *
 * <p>Deliberately imposes no {@code @ClusterScope} and does not override {@code getPragmas()} — both
 * vary per concrete test, so subclasses keep their own.
 */
public abstract class AbstractExternalDataSourceIT extends AbstractEsqlIntegTestCase {

    protected static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    /** Default data-source name used by the {@link #registerDataset(String, String, Map)} convenience. */
    private static final String SHARED_TEST_DATA_SOURCE = "test_ds";

    private final Set<String> registeredDatasets = new LinkedHashSet<>();
    private final Set<String> registeredDataSources = new LinkedHashSet<>();

    /**
     * {@link EsqlPluginWithEnterpriseOrTrialLicense} overrides {@link ExtensiblePlugin#loadExtensions}
     * with a no-op to avoid clashing with {@code EsqlPlugin}'s SPI discoverer; re-delegating to
     * {@code super} lets {@code MockPluginsService} aggregate the data-source SPI implementations so
     * the format readers / storage providers added via {@link #formatPlugins()} are discovered.
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    /** Minimal pass-through validator registered for type {@code test}; accepts any resource scheme. */
    public static final class TestDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
            return Map.of("test", new TestValidator());
        }
    }

    private static final class TestValidator implements DataSourceValidator {
        @Override
        public String type() {
            return "test";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            Map<String, DataSourceSetting> out = new HashMap<>();
            for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
                out.put(e.getKey(), new DataSourceSetting(e.getValue(), e.getKey().startsWith("secret_")));
            }
            return out;
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            return datasetSettings == null ? Map.of() : new HashMap<>(datasetSettings);
        }
    }

    /** Reader plugin(s) backing the format(s) under test, registered as node plugins for SPI discovery. */
    protected abstract Collection<Class<? extends Plugin>> formatPlugins();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(TestDataSourcePlugin.class);
        plugins.addAll(formatPlugins());
        return plugins;
    }

    /**
     * Every subclass fixture lives under a JVM-local temp directory read back via a {@code file://}
     * dataset, so the local-access allowlist must cover it; {@code createTempDir()}'s parent is the
     * shared per-JVM temp root, so allowing it covers every subclass-created temp dir.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    /**
     * Gates every subclass on the {@code dataset-in-from-command} capability, which also gates
     * {@code FROM <dataset>} resolution, plus the local-filesystem feature flag every subclass relies on for
     * its {@code file://} fixtures. Mirrors {@code FromDatasetIT.requireFeatureFlag}, so subclasses no longer
     * repeat the assume.
     */
    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
    }

    // ---------------------------------------------------------------------------------------------
    // Data-source / dataset registration helpers (auto-cleaned in cleanupRegistry()).
    // ---------------------------------------------------------------------------------------------

    /** Registers a {@code test}-type data source and records it for teardown. */
    protected void registerDataSource(String name, Map<String, Object> settings) {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, name, "test", null, new HashMap<>(settings))
            )
        );
        registeredDataSources.add(name);
    }

    /** Registers a dataset against an existing data source and records it for teardown. */
    protected void registerDataset(String name, String dataSource, String resourceUri, Map<String, Object> settings) {
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, dataSource, resourceUri, null, new HashMap<>(settings))
            )
        );
        registeredDatasets.add(name);
    }

    /** Convenience: registers {@code name} against the shared {@code test} data source, creating it on first use. */
    protected String registerDataset(String name, String resourceUri, Map<String, Object> settings) {
        if (registeredDataSources.contains(SHARED_TEST_DATA_SOURCE) == false) {
            registerDataSource(SHARED_TEST_DATA_SOURCE, Map.of());
        }
        registerDataset(name, SHARED_TEST_DATA_SOURCE, resourceUri, settings);
        return name;
    }

    /**
     * Registers a STRICT ({@code dynamic:false}) dataset with a declared mapping against the shared data source,
     * creating it on first use, and records it for teardown. The declared columns are the entire schema — strict
     * resolution reads no file to infer it. Used by the strict declared-schema tests.
     */
    protected String registerStrictDataset(
        String name,
        String resourceUri,
        LinkedHashMap<String, DatasetFieldMapping> properties,
        Map<String, Object> settings
    ) {
        if (registeredDataSources.contains(SHARED_TEST_DATA_SOURCE) == false) {
            registerDataSource(SHARED_TEST_DATA_SOURCE, Map.of());
        }
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    name,
                    SHARED_TEST_DATA_SOURCE,
                    resourceUri,
                    null,
                    new HashMap<>(settings),
                    mapping
                )
            )
        );
        registeredDatasets.add(name);
        return name;
    }

    @After
    public void cleanupRegistry() {
        for (String dataset : registeredDatasets) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { dataset }))
                    .get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", dataset, e);
            }
        }
        for (String dataSource : registeredDataSources) {
            try {
                client().execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { dataSource })
                ).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted
            } catch (Exception e) {
                logger.warn("data source cleanup [{}] failed", dataSource, e);
            }
        }
        registeredDatasets.clear();
        registeredDataSources.clear();
    }

    // ---------------------------------------------------------------------------------------------
    // Shared fixture writers.
    // ---------------------------------------------------------------------------------------------

    /**
     * Writes a Parquet fixture for the given Parquet schema, filling each of {@code rowCount} rows via
     * {@code filler} (which receives the {@link Group} to populate and the 0-based row index). Uses an
     * in-memory {@link #createOutputFile} and the uncompressed codec.
     */
    protected static Path writeParquet(Path target, String schemaText, int rowCount, int rowGroupSize, ObjIntConsumer<Group> filler)
        throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                filler.accept(g, i);
                writer.write(g);
            }
        }

        Files.write(target, baos.toByteArray());
        return target;
    }

    /**
     * Convenience overload for the canonical {@code {id:int64, name:binary(UTF8), value:int32}} fixture:
     * row {@code i} carries {@code id=i}, {@code name="row_"+i}, {@code value=i*10}.
     */
    protected static Path writeParquet(Path target, int rowCount, int rowGroupSize) throws IOException {
        return writeParquet(
            target,
            "message test { required int64 id; required binary name (UTF8); required int32 value; }",
            rowCount,
            rowGroupSize,
            (g, i) -> {
                g.add("id", (long) i);
                g.add("name", "row_" + i);
                g.add("value", i * 10);
            }
        );
    }

    /** Writes {@code content} to {@code target} through a {@link GZIPOutputStream}. */
    protected static Path writeGzipped(Path target, String content) throws IOException {
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(target))) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return target;
    }

    /**
     * An in-memory Parquet {@link OutputFile} backed by {@code baos}. Parquet needs random-access-ish
     * position tracking on write; this minimal implementation reports the running byte position.
     */
    protected static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        baos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        baos.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Shared Hive-partition fixture + external-scan profile assertions.
    // ---------------------------------------------------------------------------------------------

    /**
     * Writes a single-column ({@code id: int32}) Parquet file named {@code data.parquet} under {@code dir}, carrying
     * {@code rowCount} rows with ids {@code 0..rowCount-1}. The shared fixture for Hive-partition tests: the partition
     * column is path-derived, so the file payload only needs one trivial data column.
     */
    protected static Path writeSingleColumnIdParquet(Path dir, int rowCount) throws IOException {
        Files.createDirectories(dir);
        return writeParquet(dir.resolve("data.parquet"), "message test { required int32 id; }", rowCount, 1024, (g, i) -> g.add("id", i));
    }

    /**
     * The set of data-node names whose profile contains an external-source scan operator, identified by its
     * {@link AsyncExternalSourceOperator.Status} (the same type-based idiom the sibling profile ITs use, rather than a
     * rename-brittle operator-name string). Requires the query to have run with {@code profile(true)}. Callers assert
     * on the set: a non-empty set proves the read scanned (rather than warm-folding to a constant), and {@code >= 2}
     * distinct names proves it distributed across data nodes rather than short-circuiting coordinator-local.
     */
    protected static Set<String> externalScanNodeNames(EsqlQueryResponse response) {
        assertThat("query must be run with profile(true) to inspect the external scan", response.profile(), notNullValue());
        Set<String> nodes = new LinkedHashSet<>();
        for (var driver : response.profile().drivers()) {
            for (var op : driver.operators()) {
                if (op.status() instanceof AsyncExternalSourceOperator.Status) {
                    nodes.add(driver.nodeName());
                }
            }
        }
        return nodes;
    }
}
