/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.docvalues.ParquetTestFiles;
import org.elasticsearch.xpack.parquet.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.mount.ParquetMountRequest;
import org.elasticsearch.xpack.parquet.mount.ParquetMountResponse;
import org.elasticsearch.xpack.parquet.mount.PreviewParquetAction;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;

/**
 * The mount API: a preview of the files, and a mount that creates a data stream whose backing indices attach row groups of the files,
 * which ES|QL then reads across all of them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetMountIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateParquetDocValues.class, DataStreamsPlugin.class, EncryptionPlugin.class, EsqlPlugin.class);
    }

    private void setup() throws Exception {
        internalCluster().startNode();
        ParquetMounts.writeFileAndRegisterRepository(this);
    }

    /** ts holds epoch seconds in the test file, so it becomes @timestamp with a conversion. */
    private static Map<String, Object> body(Object... extra) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("repository", ParquetMounts.REPOSITORY);
        body.put("paths", List.of(ParquetMounts.PATH));
        body.put("timestamp_field", "ts");
        body.put("fields", Map.of("ts", Map.of("type", "date", "conversion", "seconds_to_millis")));
        body.put("index_settings", Map.of("index.number_of_replicas", 0));
        for (int i = 0; i < extra.length; i += 2) {
            body.put((String) extra[i], extra[i + 1]);
        }
        return body;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> run(org.elasticsearch.action.ActionType<ParquetMountResponse> action, Map<String, Object> body) {
        ParquetMountResponse response = client().execute(action, new ParquetMountRequest(body)).actionGet(60, TimeUnit.SECONDS);
        return response.body();
    }

    private static List<List<Object>> esql(String query) {
        try (
            EsqlQueryResponse response = client().execute(EsqlQueryAction.INSTANCE, new EsqlQueryRequest().query(query))
                .actionGet(60, TimeUnit.SECONDS)
        ) {
            List<List<Object>> rows = new ArrayList<>();
            Iterator<Iterator<Object>> iterator = response.values();
            while (iterator.hasNext()) {
                List<Object> values = new ArrayList<>();
                iterator.next().forEachRemaining(v -> values.add(v instanceof Integer i ? (Object) i.longValue() : v));
                rows.add(values);
            }
            return rows;
        }
    }

    @SuppressWarnings("unchecked")
    public void testPreviewSaysWhatAMountWouldCreateAndCreatesNothing() throws Exception {
        setup();
        Map<String, Object> preview = run(PreviewParquetAction.INSTANCE, body("max_rows_per_index", 2_000));

        assertEquals((long) ParquetMounts.ROWS, preview.get("row_count"));
        List<Map<String, Object>> files = (List<Map<String, Object>>) preview.get("files");
        assertEquals(1, files.size());
        assertEquals(ParquetMounts.PATH, files.get(0).get("path"));
        assertEquals(4, files.get(0).get("row_groups"));

        Map<String, Object> properties = (Map<String, Object>) ((Map<String, Object>) preview.get("mapping")).get("properties");
        assertThat(properties.keySet(), hasItems("id", "@timestamp", "low", "bucket", "maybe", "name", "kind", "note"));
        assertEquals(Map.of("type", "date", "index", false), properties.get("@timestamp"));
        assertEquals(Map.of("type", "keyword", "index", false, "multi_value", false), properties.get("name"));
        assertEquals(List.of(), preview.get("unmapped"));

        // 4 row groups of 1000 rows with a limit of 2000: two indices
        List<Map<String, Object>> indices = (List<Map<String, Object>>) preview.get("indices");
        assertEquals(2, indices.size());
        assertEquals(2_000L, indices.get(0).get("rows"));
        assertEquals(
            List.of(Map.of("path", ParquetMounts.PATH, "first_row_group", 2, "row_group_count", 2, "rows", 1_500L)),
            indices.get(1).get("pieces")
        );

        assertEquals("a preview creates no index", 0, clusterService().state().metadata().getProject().indices().size());
    }

    public void testMountCreatesADataStreamOverIndicesThatAttachTheRowGroups() throws Exception {
        setup();
        Map<String, Object> mounted = run(MountParquetAction.INSTANCE, body("data_stream", "events", "max_rows_per_index", 2_000));
        assertEquals(true, mounted.get("acknowledged"));
        assertEquals("events", mounted.get("data_stream"));
        assertEquals(2, ((List<?>) mounted.get("backing_indices")).size());

        DataStream dataStream = client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "events" })
        ).actionGet().getDataStreams().get(0).getDataStream();
        assertEquals(2, dataStream.getIndices().size());
        ensureGreen("events");
        for (var index : dataStream.getIndices()) {
            IndexMetadata metadata = clusterService().state().metadata().getProject().index(index);
            assertEquals("columnar", metadata.getSettings().get("index.mode"));
            assertEquals(1, metadata.getNumberOfShards());
            assertEquals(ParquetMounts.REPOSITORY, metadata.getSettings().get(ParquetBackedSettings.REPOSITORY.getKey()));
        }

        // ES|QL reads the whole data stream: the first index holds rows 0 to 1999 and the second rows 2000 to 3499
        assertEquals(List.of(List.of((long) ParquetMounts.ROWS)), esql("FROM events | STATS c = COUNT(*)"));
        assertEquals(
            List.of(List.of(3_499L, 0L, 1_500L)),
            esql("FROM events | STATS max_id = MAX(id), min_id = MIN(id), second = COUNT(*) WHERE id >= 2000")
        );
        // @timestamp is ts in seconds converted to milliseconds: 3499 * 1000 seconds after the epoch
        assertEquals(
            List.of(List.of("1970-01-01T00:00:00.000Z", "1970-02-10T11:56:40.000Z")),
            esql(
                "FROM events | STATS earliest = MIN(@timestamp), latest = MAX(@timestamp) "
                    + "| EVAL earliest = TO_STRING(earliest), latest = TO_STRING(latest)"
            )
        );
        // the _id is the row number in each backing index, so the same number exists in both
        assertEquals(List.of(List.of(2L)), esql("FROM events METADATA _id | WHERE _id == \"7\" | STATS c = COUNT(*)"));
    }

    public void testBadRequestsAreRejectedWithAReasonAndCreateNothing() throws Exception {
        setup();
        IllegalArgumentException noTimestamp = expectThrows(
            IllegalArgumentException.class,
            () -> run(MountParquetAction.INSTANCE, body("data_stream", "events", "fields", Map.of(), "timestamp_field", null))
        );
        assertThat(noTimestamp.getMessage(), containsString("needs a [@timestamp] field"));

        Map<String, Object> unknown = body("data_stream", "events");
        unknown.put("nope", 1);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> run(MountParquetAction.INSTANCE, unknown)).getMessage(),
            containsString("unknown field [nope]")
        );

        Map<String, Object> escape = body("data_stream", "events");
        escape.put("paths", List.of("../secret.parquet"));
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> run(MountParquetAction.INSTANCE, escape)).getMessage(),
            containsString("below the base path")
        );

        Map<String, Object> missingDataStream = body();
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> run(MountParquetAction.INSTANCE, missingDataStream)).getMessage(),
            containsString("[data_stream] or [index] is required")
        );

        Map<String, Object> controlled = body("data_stream", "events", "index_settings", Map.of("index.parquet.repository", "x"));
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> run(MountParquetAction.INSTANCE, controlled)).getMessage(),
            containsString("decided by the mount")
        );

        Map<String, Object> missingFile = body("data_stream", "events");
        missingFile.put("paths", List.of("events/missing.parquet"));
        expectThrows(org.elasticsearch.ResourceNotFoundException.class, () -> run(MountParquetAction.INSTANCE, missingFile));

        assertEquals(0, clusterService().state().metadata().getProject().indices().size());
        assertTrue(clusterService().state().metadata().getProject().dataStreams().isEmpty());
    }

    public void testAFailedMountDeletesWhatItCreatedAndCanBeRepeated() throws Exception {
        setup();
        // the name of the first backing index is taken, so the mount fails after it has created the template
        String firstBackingIndex = DataStream.getDefaultBackingIndexName("events", 1, System.currentTimeMillis());
        assertAcked(prepareCreate(firstBackingIndex).setSettings(Settings.builder().put("index.number_of_replicas", 0)));
        expectThrows(ResourceAlreadyExistsException.class, () -> run(MountParquetAction.INSTANCE, body("data_stream", "events")));

        assertTrue(clusterService().state().metadata().getProject().dataStreams().isEmpty());
        expectThrows(
            org.elasticsearch.ResourceNotFoundException.class,
            () -> client().execute(
                GetComposableIndexTemplateAction.INSTANCE,
                new GetComposableIndexTemplateAction.Request(TEST_REQUEST_TIMEOUT, "parquet-mount-events")
            ).actionGet()
        );

        // with the name free again, the same request works
        assertAcked(indicesAdmin().prepareDelete(firstBackingIndex));
        assertEquals(true, run(MountParquetAction.INSTANCE, body("data_stream", "events")).get("acknowledged"));
        assertEquals(List.of(List.of((long) ParquetMounts.ROWS)), esql("FROM events | STATS c = COUNT(*)"));
        expectThrows(ResourceAlreadyExistsException.class, () -> run(MountParquetAction.INSTANCE, body("data_stream", "events")));
        expectThrows(IndexNotFoundException.class, () -> indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices("nope").get());
    }

    /**
     * Two files fill one backing index: the first holds 3,500 rows and the second 1,200 rows in row groups of 500, and the limit of
     * 4,500 rows takes the first file and two row groups of the second into the first index, and the last row group into the second.
     * The row number that is the {@code _id} then runs on over the files inside one index.
     */
    public void testAnIndexThatHoldsPiecesOfSeveralFilesNumbersItsRowsAcrossThem() throws Exception {
        internalCluster().startNode();
        Path location = ParquetMounts.writeFileAndRegisterRepository(this);
        String second = "events/part-0002.parquet";
        Files.write(location.resolve(second), ParquetTestFiles.writeFlat(1_200, 500));

        Map<String, Object> body = body("data_stream", "events", "max_rows_per_index", 4_500);
        body.put("paths", List.of(ParquetMounts.PATH, second));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plan = (List<Map<String, Object>>) run(PreviewParquetAction.INSTANCE, body).get("indices");
        assertEquals(2, plan.size());
        assertEquals(4_500L, plan.get(0).get("rows"));
        assertEquals(2, ((List<?>) plan.get(0).get("pieces")).size());
        assertEquals(1, ((List<?>) plan.get(1).get("pieces")).size());

        assertEquals(true, run(MountParquetAction.INSTANCE, body).get("acknowledged"));
        ensureGreen("events");

        assertEquals(List.of(List.of(4_700L)), esql("FROM events | STATS c = COUNT(*)"));
        // the id column restarts in the second file: row 3600 of the first index is row 100 of the second file
        assertEquals(List.of(List.of(100L)), esql("FROM events METADATA _id | WHERE _id == \"3600\" | KEEP id"));
        assertEquals(List.of(List.of(999L)), esql("FROM events METADATA _id | WHERE _id == \"4499\" | KEEP id"));
        // the last row group of the second file is the whole second index: its rows 0 to 199 are the rows 1000 to 1199 of the file
        assertEquals(
            List.of(List.of(1_000L, 1_199L, 200L)),
            esql("FROM events METADATA _index | WHERE _index LIKE \"*000002\" | STATS lo = MIN(id), hi = MAX(id), c = COUNT(*)")
        );
    }

    /**
     * An {@code index} mount makes one plain columnar index: the columns keep their names, no timestamp field is needed, there is no
     * data stream, and files that need more than one index are refused with the way out.
     */
    public void testMountAsAPlainIndexKeepsTheColumnNames() throws Exception {
        setup();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("repository", ParquetMounts.REPOSITORY);
        body.put("paths", List.of(ParquetMounts.PATH));
        body.put("index", "events");
        body.put("fields", Map.of("ts", Map.of("type", "date", "conversion", "seconds_to_millis")));
        body.put("index_settings", Map.of("index.number_of_replicas", 0));

        Map<String, Object> tooSmall = new LinkedHashMap<>(body);
        tooSmall.put("max_rows_per_index", 2_000);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> run(MountParquetAction.INSTANCE, tooSmall)).getMessage(),
            containsString("use [data_stream]")
        );
        Map<String, Object> both = new LinkedHashMap<>(body);
        both.put("data_stream", "events");
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> run(MountParquetAction.INSTANCE, both)).getMessage(),
            containsString("not both")
        );
        assertEquals(0, clusterService().state().metadata().getProject().indices().size());

        Map<String, Object> mounted = run(MountParquetAction.INSTANCE, body);
        assertEquals("events", mounted.get("index"));
        ensureGreen("events");
        assertTrue(clusterService().state().metadata().getProject().dataStreams().isEmpty());
        assertEquals(1, clusterService().state().metadata().getProject().indices().size());

        assertEquals(List.of(List.of((long) ParquetMounts.ROWS)), esql("FROM events | STATS c = COUNT(*)"));
        // ts keeps its name, and is a date because of the override
        assertEquals(List.of(List.of("1970-02-10T11:56:40.000Z")), esql("FROM events | STATS hi = MAX(ts) | EVAL hi = TO_STRING(hi)"));
        assertEquals(List.of(List.of(1_000L)), esql("FROM events | WHERE id >= 2000 AND id < 3000 | STATS c = COUNT(*)"));
        expectThrows(ResourceAlreadyExistsException.class, () -> run(MountParquetAction.INSTANCE, body));
    }
}
