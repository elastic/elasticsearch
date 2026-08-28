/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end: a field ({@code user}) that is a scalar in some NDJSON records and a JSON object in others
 * is not a shape conflict. The object record null-fills {@code user} and the query succeeds under every
 * {@code error_mode}, including STRICT. Runs through a real {@code FROM <dataset>} query.
 */
public class NdJsonScalarObjectConflictIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

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

    private Path fixture;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(NdJsonDataSourcePlugin.class);
        plugins.add(TestDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
    }

    /**
     * {@code user} is a string in two records and a nested object in one. Registers {@code strict_ds}
     * (default error policy) and {@code lenient_ds} ({@code error_mode: skip_row}).
     */
    @Before
    public void writeFixtureAndRegister() throws Exception {
        fixture = createTempDir().resolve("scalar-then-object.ndjson");
        Files.writeString(
            fixture,
            String.join(
                "\n",
                "{\"event\":1,\"user\":\"alice\"}",
                "{\"event\":2,\"user\":{\"id\":\"bob\",\"tier\":\"gold\"}}",
                "{\"event\":3,\"user\":\"carol\"}",
                ""
            )
        );
        String resource = StoragePath.fileUri(fixture);
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(PutDatasetAction.INSTANCE, putDatasetRequest("strict_ds", "local_ds", resource, Map.of("format", "ndjson")))
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("lenient_ds", "local_ds", resource, Map.of("format", "ndjson", "error_mode", "skip_row"))
            )
        );
    }

    @After
    public void cleanupRegistry() throws Exception {
        for (String dataset : List.of("strict_ds", "lenient_ds")) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(dataset)).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", dataset, e);
            }
        }
        try {
            client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest("local_ds")).get(30, TimeUnit.SECONDS);
        } catch (ResourceNotFoundException ignored) {
            // already deleted
        } catch (Exception e) {
            logger.warn("data source cleanup [local_ds] failed", e);
        }
        Files.deleteIfExists(fixture);
    }

    /**
     * STRICT keeps every record. The object-valued {@code user} null-fills that column; the two scalar
     * records keep their strings.
     */
    public void testStrictKeepsObjectRecordWithNullUser() {
        try (var response = run(syncEsqlQueryRequest("FROM strict_ds | KEEP event, user | SORT event"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(3));
            assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
            assertThat(rows.get(0).get(1), equalTo("alice"));
            assertThat(((Number) rows.get(1).get(0)).intValue(), equalTo(2));
            assertNull(rows.get(1).get(1));
            assertThat(((Number) rows.get(2).get(0)).intValue(), equalTo(3));
            assertThat(rows.get(2).get(1), equalTo("carol"));
        }
    }

    /**
     * {@code skip_row} has nothing to drop: a scalar/object mix is not a value error.
     */
    public void testSkipRowAlsoKeepsObjectRecord() {
        try (var response = run(syncEsqlQueryRequest("FROM lenient_ds | KEEP event, user | SORT event"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(3));
            assertThat(rows.get(0).get(1), equalTo("alice"));
            assertNull(rows.get(1).get(1));
            assertThat(rows.get(2).get(1), equalTo("carol"));
        }
    }

    private static PutDataSourceAction.Request putDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, name, "test", null, new HashMap<>(settings));
    }

    private static PutDatasetAction.Request putDatasetRequest(
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) {
        return new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, dataSource, resource, null, new HashMap<>(settings));
    }

    private static DeleteDataSourceAction.Request deleteDataSourceRequest(String name) {
        return new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }

    private static DeleteDatasetAction.Request deleteDatasetRequest(String name) {
        return new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }
}
