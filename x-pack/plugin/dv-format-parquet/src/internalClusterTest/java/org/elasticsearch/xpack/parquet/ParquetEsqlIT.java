/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * ES|QL over an index that mounts a Parquet file: the fields are read through the block loaders of the mapped field types, over the
 * doc values that the stub segment serves. The index is columnar, so a string is a {@code keyword} with binary doc values.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetEsqlIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateParquetDocValues.class, EncryptionPlugin.class, EsqlPlugin.class);
    }

    private void mount() throws Exception {
        internalCluster().startNode();
        ParquetMounts.writeFileAndRegisterRepository(this);
        assertAcked(
            prepareCreate("events").setSettings(
                Settings.builder()
                    .put("index.mode", "columnar")
                    .put(org.elasticsearch.index.IndexSettings.ID_FIELD_INDEXED.getKey(), false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(ParquetMounts.mountSettings(ParquetMounts.COLUMNAR_FIELDS).build())
            ).setMapping(ParquetMounts.COLUMNAR_MAPPING)
        );
        ensureGreen("events");
    }

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(ParquetEsqlIT.class);

    private static List<List<Object>> esql(String query) {
        try (
            EsqlQueryResponse response = client().execute(EsqlQueryAction.INSTANCE, new EsqlQueryRequest().query(query))
                .actionGet(60, TimeUnit.SECONDS)
        ) {
            List<List<Object>> rows = new ArrayList<>();
            Iterator<Iterator<Object>> iterator = response.values();
            while (iterator.hasNext()) {
                List<Object> values = new ArrayList<>();
                iterator.next().forEachRemaining(values::add);
                rows.add(values);
            }
            LOGGER.info("--> [{}] gives [{}]", query, rows.size() > 8 ? rows.subList(0, 8) + "..." : rows);
            return rows;
        }
    }

    public void testAggregationsOverNumbers() throws Exception {
        mount();
        assertEquals(List.of(List.of(3_500L)), esql("FROM events | STATS c = COUNT(*)"));
        assertEquals(
            List.of(List.of(0L, 3_499L, 3_499_000L, 6_123_250L)),
            esql("FROM events | STATS min_status = MIN(status), max_status = MAX(status), max_ts = MAX(ts), sum_status = SUM(status)")
        );
    }

    public void testOptionalFieldsAreNullWhereTheyHaveNoValue() throws Exception {
        mount();
        // 3 of 4 rows have a note (2625), and 2 of 3 rows have a maybe (2333)
        assertEquals(List.of(List.of(2_625L, 2_333L)), esql("FROM events | STATS notes = COUNT(note), maybes = COUNT(maybe)"));
        assertEquals(List.of(List.of(875L)), esql("FROM events | WHERE note IS NULL | STATS c = COUNT(*)"));
    }

    public void testStringsAreFilteredGroupedAndSorted() throws Exception {
        mount();
        assertEquals(List.of(List.of(700L)), esql("FROM events | WHERE kind == \"k1\" | STATS c = COUNT(*)"));
        assertEquals(
            // the aggregate comes before the grouping key in the columns
            List.of(List.of(700L, "k0"), List.of(700L, "k1"), List.of(700L, "k2"), List.of(700L, "k3"), List.of(700L, "k4")),
            esql("FROM events | STATS c = COUNT(*) BY kind | SORT kind")
        );
        assertEquals(List.of(List.of("n1234", 1234L)), esql("FROM events | WHERE name == \"n1234\" | KEEP name, status"));
        assertEquals(List.of(List.of("n0"), List.of("n1"), List.of("n10")), esql("FROM events | SORT name | LIMIT 3 | KEEP name"));
    }

    public void testRowsWithTheirIdAndOptionalValues() throws Exception {
        mount();
        List<List<Object>> rows = esql(
            "FROM events METADATA _id | WHERE status >= 3 AND status <= 5 | SORT status | KEEP _id, status, ts, maybe, note"
        );
        assertEquals(
            List.of(
                java.util.Arrays.asList("3", 3L, 3_000L, null, "note-3"),
                java.util.Arrays.asList("4", 4L, 4_000L, 4L, null),
                java.util.Arrays.asList("5", 5L, 5_000L, 5L, "note-5")
            ),
            rows
        );
    }
}
