/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code unmapped_fields="LOAD_ALL"} returns one {@code _unmapped_fields} column whose content each shard contributes independently:
 * unlike the per-field layout it replaced, no union of field names across shards is needed. This checks that rows from different
 * shards, each carrying different unmapped fields, all land in the same single column with their own leaves and nothing from another
 * shard's rows.
 */
public class LoadAllMultiShardIT extends AbstractEsqlIntegTestCase {

    public void testEveryShardFeedsTheSameSingleColumn() {
        assumeTrue("requires the LOAD_ALL single column", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_SINGLE_SOURCE_COLUMN.isEnabled());
        internalCluster().ensureAtLeastNumDataNodes(2);
        int shards = 3;
        assertAcked(client().admin().indices().prepareCreate("load-all-shards").setSettings(indexSettings(shards, 0)).setMapping("""
            { "dynamic": false, "properties": { "mapped": { "type": "long" } } }"""));
        int docs = 12;
        for (int i = 0; i < docs; i++) {
            // Every document names its own unmapped field, so no two shards share one, plus one nested leaf common to all.
            indexDoc("load-all-shards", Integer.toString(i), "mapped", i, "only_" + i, "v" + i, "common", Map.of("n", i));
        }
        refresh("load-all-shards");

        try (EsqlQueryResponse response = run("SET unmapped_fields=\"LOAD_ALL\"; FROM load-all-shards | SORT mapped | LIMIT 100")) {
            List<String> columns = response.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, equalTo(List.of("mapped", "_unmapped_fields")));

            List<List<Object>> rows = new ArrayList<>();
            response.rows().forEach(row -> {
                List<Object> values = new ArrayList<>();
                row.forEach(values::add);
                rows.add(values);
            });
            assertThat(rows.size(), equalTo(docs));
            for (int i = 0; i < docs; i++) {
                List<Object> row = rows.get(i);
                assertThat(((Number) row.get(0)).intValue(), equalTo(i));
                Map<String, Object> expected = new LinkedHashMap<>();
                expected.put("only_" + i, "v" + i);
                expected.put("common.n", i);
                Object actual = row.get(1);
                assertThat("row " + i, actual, equalTo(expected));
            }
        }
    }
}
