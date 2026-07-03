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
import org.elasticsearch.cluster.metadata.DatasetMetadata;
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
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
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
 * End-to-end reproduction of elastic/esql-planning#1028, run through a real {@code FROM <dataset>}
 * query (planner + execution + client response), not just the reader-level unit tests in
 * {@code NdJsonPageIteratorTests}/{@code NdJsonPageDecoderTests}. A field ("user") that is a scalar in
 * some sampled NDJSON records and a JSON object in others must, per {@link ErrorPolicy}: fail the query
 * under the default strict policy with an actionable client-visible error, or null-fill just that field
 * and surface a client-visible {@code Warning} under a non-strict policy, while the rest of the record
 * (and the other rows) still return normally. This proves the reader's {@code ErrorPolicy} routing and
 * {@code SkipWarnings} actually propagate all the way to the client through
 * {@code AsyncExternalSourceOperator} and the transport response, not just to a hand-bound test
 * {@code ThreadContext} (mirroring {@code FromDatasetIT} and {@code WarningsIT}).
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
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
    }

    /**
     * The exact repro shape from elastic/esql-planning#1028: {@code user} is a plain string in most
     * records but a nested object in one of them. Scalar-first-wins schema inference resolves {@code user}
     * to {@code KEYWORD}, so the object-valued record is the one that hits the shape conflict at decode
     * time. Registers a data source and two datasets over the same fixture — {@code strict_ds} with the
     * default (strict) error policy and {@code lenient_ds} with {@code error_mode: skip_row} — so each
     * test just picks the dataset matching the policy it exercises.
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
     * Default (strict) policy: reaching the scalar/object conflict must fail the whole query with an
     * actionable message naming the conflicting field and both shapes, mirroring how core ES dynamic
     * mapping rejects the same ambiguity as a hard document-parsing conflict.
     */
    public void testStrictPolicyFailsOnScalarObjectConflict() {
        String query = "FROM strict_ds | KEEP event, user | SORT event";
        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query), TIMEOUT).close());
        String trace = ExceptionsHelper.stackTrace(e);
        assertTrue("strict policy must fail on the shape conflict, got: " + trace, trace.contains("user"));
        assertTrue("error must explain the conflict, got: " + trace, trace.contains("resolved to scalar type"));
    }

    /**
     * Non-strict policy ({@code error_mode: skip_row}, set on {@code lenient_ds}): the conflicting
     * record's {@code user} column is null-filled and the rest of that record (and every other row)
     * still returns, instead of failing the whole query or dropping the row outright —
     * elastic/esql-planning#1028 notes the conflict path already decodes the rest of the record. The
     * query runs through a chosen coordinator and we read that node's accumulated response
     * {@code Warning} headers, proving the warning recorded by the reader propagates all the way to
     * the client.
     */
    public void testSkipRowPolicyNullFillsAndWarnsClient() throws Exception {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM lenient_ds | KEEP event, user | SORT event");

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<List<Object>>> values = new AtomicReference<>();
        AtomicReference<List<? extends ColumnInfo>> columns = new AtomicReference<>();
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        // ActionListener.wrap (not the run() helper) so we can also read the coordinator's response
        // Warning headers; the transport client owns the response ref-count, so we must not close it
        // here (that would double-decRef -- see ExternalCsvHivePartitionedIT).
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
        assertThat("all three records must still return, just [user] is null for the conflicting one", rows.size(), equalTo(3));
        assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(1));
        assertThat(rows.get(0).get(1), equalTo("alice"));
        assertThat("the conflicting record's [event] sibling column must survive", ((Number) rows.get(1).get(0)).intValue(), equalTo(2));
        assertThat("the conflicting record's [user] must be null-filled, not the whole row dropped", rows.get(1).get(1), nullValue());
        assertThat(((Number) rows.get(2).get(0)).intValue(), equalTo(3));
        assertThat(rows.get(2).get(1), equalTo("carol"));

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
