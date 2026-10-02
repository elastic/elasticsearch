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

import java.nio.charset.StandardCharsets;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end: one file's scalar {@code user} and another's nested {@code user.id}/{@code user.tier}.
 * Explicit {@code union_by_name} (and a legacy stored document missing the key) keep both files'
 * columns; {@code first_file_wins} drops later dotted columns. Runs through a real {@code FROM <dataset>}
 * query.
 */
public class NdJsonCrossFileScalarAndDottedColumnsIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private static final String FILE_DS = "file_ds";

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

    private Path fixtureDir;

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
     * {@code a.ndjson}'s {@code user} is a string; {@code b.ndjson}'s is a nested object.
     * Registers explicit UBN datasets, explicit FFW, a legacy omit-key document (TestValidator),
     * and a new omit-key PUT through {@code FileDataSourceValidator} (stored FFW).
     */
    @Before
    public void writeFixtureAndRegister() throws Exception {
        fixtureDir = createTempDir().resolve("cross_file_scalar_and_dotted");
        Files.createDirectories(fixtureDir);
        Files.writeString(fixtureDir.resolve("a.ndjson"), "{\"event\":1,\"user\":\"alice\"}\n", StandardCharsets.UTF_8);
        Files.writeString(
            fixtureDir.resolve("b.ndjson"),
            "{\"event\":2,\"user\":{\"id\":\"bob\",\"tier\":\"gold\"}}\n",
            StandardCharsets.UTF_8
        );
        String resource = StoragePath.fileUri(fixtureDir) + "/*.ndjson";
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("default_ds", "local_ds", resource, Map.of("format", "ndjson", "schema_resolution", "union_by_name"))
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest(
                    "skip_row_ds",
                    "local_ds",
                    resource,
                    Map.of("format", "ndjson", "error_mode", "skip_row", "schema_resolution", "union_by_name")
                )
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest(
                    "ffw_ds",
                    "local_ds",
                    resource,
                    Map.of("format", "ndjson", "schema_resolution", "first_file_wins", "error_mode", "null_field", "file_sort_by", "name")
                )
            )
        );
        assertAcked(
            client().execute(PutDatasetAction.INSTANCE, putDatasetRequest("legacy_ds", "local_ds", resource, Map.of("format", "ndjson")))
        );
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putLocalFileDataSourceRequest()));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest(
                    "omit_put_ds",
                    FILE_DS,
                    resource,
                    Map.of("format", "ndjson", "error_mode", "null_field", "file_sort_by", "name")
                )
            )
        );
    }

    @After
    public void cleanupRegistry() throws Exception {
        for (String dataset : List.of("default_ds", "skip_row_ds", "ffw_ds", "legacy_ds", "omit_put_ds")) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(dataset)).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", dataset, e);
            }
        }
        for (String dataSource : List.of("local_ds", FILE_DS)) {
            try {
                client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(dataSource)).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted
            } catch (Exception e) {
                logger.warn("data source cleanup [{}] failed", dataSource, e);
            }
        }
        Files.walk(fixtureDir).sorted((a, b) -> b.compareTo(a)).forEach(p -> {
            try {
                Files.deleteIfExists(p);
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        });
    }

    /**
     * Explicit {@code union_by_name} keeps both files: a scalar {@code user} and dotted
     * {@code user.id}/{@code user.tier} are independent columns, not a value error. The object file
     * null-fills {@code user}.
     */
    public void testDefaultSettingsKeepsBothFiles() {
        try (var response = run(syncEsqlQueryRequest("FROM default_ds | KEEP event, user, `user.id`, `user.tier` | SORT event"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(2));
            assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
            assertThat(rows.get(0).get(1), equalTo("alice"));
            assertNull(rows.get(0).get(2));
            assertNull(rows.get(0).get(3));
            assertThat(((Number) rows.get(1).get(0)).intValue(), equalTo(2));
            assertNull(rows.get(1).get(1));
            assertThat(rows.get(1).get(2), equalTo("bob"));
            assertThat(rows.get(1).get(3), equalTo("gold"));
        }
    }

    /**
     * First-file-wins takes {@code a.ndjson}'s columns only ({@code file_sort_by: name}). Later dotted
     * {@code user.id}/{@code user.tier} are not in the schema. The object file's {@code user} is
     * unreadable as the anchor's keyword and nulls under {@code error_mode: null_field}.
     */
    public void testFirstFileWinsDropsLaterDottedColumns() {
        try (var response = run(syncEsqlQueryRequest("FROM ffw_ds | KEEP event, user | SORT event"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(2));
            assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
            assertThat(rows.get(0).get(1), equalTo("alice"));
            assertThat(((Number) rows.get(1).get(0)).intValue(), equalTo(2));
            assertNull(rows.get(1).get(1));
        }
        Exception unknown = expectThrows(Exception.class, () -> {
            try (var ignored = run(syncEsqlQueryRequest("FROM ffw_ds | KEEP `user.id`"), TIMEOUT)) {}
        });
        assertThat(unknown.getMessage(), containsString("user.id"));
    }

    /**
     * A new PUT that omits {@code schema_resolution} goes through {@code FileDataSourceValidator}
     * and stores {@code first_file_wins}. Same columns as {@link #testFirstFileWinsDropsLaterDottedColumns}.
     */
    public void testOmittedKeyPutMaterializesFirstFileWins() {
        try (var response = run(syncEsqlQueryRequest("FROM omit_put_ds | KEEP event, user | SORT event"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(2));
            assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
            assertThat(rows.get(0).get(1), equalTo("alice"));
            assertThat(((Number) rows.get(1).get(0)).intValue(), equalTo(2));
            assertNull(rows.get(1).get(1));
        }
        Exception unknown = expectThrows(Exception.class, () -> {
            try (var ignored = run(syncEsqlQueryRequest("FROM omit_put_ds | KEEP `user.id`"), TIMEOUT)) {}
        });
        assertThat(unknown.getMessage(), containsString("user.id"));
    }

    /**
     * A stored dataset whose settings omit {@code schema_resolution} (legacy cluster-state document)
     * still unions: both files' columns are present.
     */
    public void testLegacyMissingKeyHydratesUnionByName() {
        try (var response = run(syncEsqlQueryRequest("FROM legacy_ds | KEEP event, user, `user.id`, `user.tier` | SORT event"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(2));
            assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
            assertThat(rows.get(0).get(1), equalTo("alice"));
            assertNull(rows.get(0).get(2));
            assertNull(rows.get(0).get(3));
            assertThat(((Number) rows.get(1).get(0)).intValue(), equalTo(2));
            assertNull(rows.get(1).get(1));
            assertThat(rows.get(1).get(2), equalTo("bob"));
            assertThat(rows.get(1).get(3), equalTo("gold"));
        }
    }

    /**
     * {@code skip_row} has nothing to drop: a scalar in one file and an object in another are
     * independent columns, not a value error.
     */
    public void testSkipRowAlsoKeepsBothFiles() {
        try (var response = run(syncEsqlQueryRequest("FROM skip_row_ds | KEEP event, user | SORT event"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(2));
            assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
            assertThat(rows.get(0).get(1), equalTo("alice"));
            assertThat(((Number) rows.get(1).get(0)).intValue(), equalTo(2));
            assertNull(rows.get(1).get(1));
        }
    }

    private static PutDataSourceAction.Request putDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, name, "test", null, new HashMap<>(settings));
    }

    private static PutDataSourceAction.Request putLocalFileDataSourceRequest() {
        return new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, FILE_DS, "local", null, new HashMap<>());
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
