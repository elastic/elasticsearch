/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end reproduction of elastic/esql-planning#1050, the cross-file completion of #1028: a
 * field that is a scalar leaf in one file's schema and a nested object (dotted-prefix parent) in
 * another's must reconcile to a single shape under the default {@code UNION_BY_NAME} schema
 * resolution, with the losing file's records routed through the same {@code ErrorPolicy}
 * machinery {@link NdJsonScalarObjectConflictIT} exercises for a single file. Runs through a real
 * {@code FROM <dataset>} query (planner + execution + client response), mirroring
 * {@code NdJsonScalarObjectConflictIT}'s dataset-registration pattern rather than the ad hoc
 * {@code EXTERNAL} command, which is slated for removal.
 */
public class NdJsonCrossFileScalarObjectConflictIT extends AbstractEsqlIntegTestCase {

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
     * The exact repro shape from esql-planning#1050: {@code a.ndjson}'s {@code user} is a plain
     * string, {@code b.ndjson}'s is a nested object. Lexicographic glob ordering (see
     * {@code GlobExpander}) makes {@code a.ndjson} the first file, so first-shape-wins schema
     * reconciliation resolves {@code user} to {@code KEYWORD} and {@code b.ndjson} is the one that
     * hits the shape conflict at decode time. Registers a data source and two datasets over the
     * same two-file directory — {@code strict_ds} with the default (strict) error policy and
     * {@code lenient_ds} with {@code error_mode: skip_row} — so each test just picks the dataset
     * matching the policy it exercises.
     */
    @Before
    public void writeFixtureAndRegister() throws Exception {
        fixtureDir = createTempDir().resolve("cross_file_shape_conflict");
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
        Files.walk(fixtureDir).sorted((a, b) -> b.compareTo(a)).forEach(p -> {
            try {
                Files.deleteIfExists(p);
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        });
    }

    /**
     * Default settings ({@code UNION_BY_NAME} schema resolution, strict error policy): before this
     * fix, {@code a.ndjson}'s scalar {@code user} and {@code b.ndjson}'s {@code user.id}/
     * {@code user.tier} would coexist in the fabricated unified schema and the query would
     * silently return {@code with_user=1} rather than failing — the correctness bug this fix
     * closes. With the fix, the family collapses to {@code a.ndjson}'s scalar shape, so
     * {@code b.ndjson}'s now-pinned scalar {@code user} attribute hits its real object value and
     * fails per elastic/esql-planning#1028's decode-time shape-conflict handling — mirroring
     * {@link NdJsonScalarObjectConflictIT#testStrictPolicyFailsOnScalarObjectConflict}.
     */
    public void testDefaultSettingsFailsOnCrossFileScalarObjectConflict() {
        String query = "FROM strict_ds | STATS total = COUNT(*), with_user = COUNT(user)";
        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query), TIMEOUT).close());
        String trace = ExceptionsHelper.stackTrace(e);
        assertTrue("must fail on the cross-file shape conflict, got: " + trace, trace.contains("user"));
        assertTrue("error must explain the conflict, got: " + trace, trace.contains("resolved to scalar type"));
    }

    /**
     * Non-strict policy ({@code error_mode: skip_row}, set on {@code lenient_ds}): {@code
     * b.ndjson}'s {@code user} column is null-filled — not the row or the whole file dropped —
     * {@code a.ndjson}'s row is unaffected, and the client receives a {@code Warning} naming the
     * conflict. Nothing silently vanishes, end to end, not just at the reconciliation-unit level.
     * The query runs through a chosen coordinator and we read that node's accumulated response
     * {@code Warning} headers, proving the warning recorded by the reader propagates all the way
     * to the client.
     */
    public void testSkipRowPolicyNullFillsAndWarnsClient() throws Exception {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM lenient_ds | KEEP event, user | SORT event");

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<List<Object>>> values = new AtomicReference<>();
        AtomicReference<List<? extends ColumnInfo>> columns = new AtomicReference<>();
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        // ActionListener.wrap (not the run() helper) so we can also read the coordinator's
        // response Warning headers; the transport client owns the response ref-count, so we must
        // not close it here (that would double-decRef -- see ExternalCsvHivePartitionedIT).
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                values.set(getValuesList(response));
                columns.set(response.columns());
                TransportService transportService = internalCluster().getInstance(TransportService.class, coordinator.getName());
                warnings.addAll(
                    transportService.getThreadPool().getThreadContext().getResponseHeaders().getOrDefault("Warning", List.of())
                );
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within 2 minutes", latch.await(2, TimeUnit.MINUTES));
        if (failure.get() != null) {
            throw new AssertionError("non-strict read must not fail, but did", failure.get());
        }

        assertThat(columns.get().size(), equalTo(2));
        assertThat(columns.get().get(0).name(), equalTo("event"));
        assertThat(columns.get().get(1).name(), equalTo("user"));

        List<List<Object>> rows = values.get();
        assertThat("both files' rows must still return, just [user] is null for the conflicting one", rows.size(), equalTo(2));
        assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
        assertThat(rows.get(0).get(1), equalTo("alice"));
        assertThat("b.ndjson's [event] sibling column must survive", ((Number) rows.get(1).get(0)).intValue(), equalTo(2));
        assertThat("b.ndjson's [user] must be null-filled, not the whole row dropped", rows.get(1).get(1), nullValue());

        assertTrue(
            "client must receive a Warning naming the conflicting field, got: " + warnings,
            warnings.stream().anyMatch(w -> w.contains("user"))
        );
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
