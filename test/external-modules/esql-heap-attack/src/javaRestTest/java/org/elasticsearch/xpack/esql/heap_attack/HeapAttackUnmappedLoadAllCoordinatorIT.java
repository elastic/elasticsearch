/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/**
 * Heap-attack coverage for the <em>coordinator</em> side of {@code SET unmapped_fields="LOAD_ALL"}.
 * <p>
 * Flattening the {@code _unmapped_fields} column happens on the node that coordinates the query, after the data nodes have produced
 * their rows. On the standard heap-attack cluster that node is also a data node with the same heap, and the data node's own
 * materialisation of the surviving {@code _source} value (which runs first, and is released before the coordinator starts) is the
 * larger of the two, so it can never be the coordinator that runs out. To isolate the coordinator this suite uses its own topology: a
 * generously sized data node that holds the shard and a small coordinating-only node that every request is sent to. Whatever the
 * data node needs, the small node has to flatten the same row with a fraction of the heap, which only works while the flattening
 * walks the JSON a leaf at a time instead of building a {@code Map}/{@code List} graph many times the row's size.
 */
public class HeapAttackUnmappedLoadAllCoordinatorIT extends HeapAttackRestHelpers {
    private static final int DATA_NODE_HEAP_MB = 2048;
    private static final int COORDINATOR_HEAP_MB = 320;
    private static final String MAPPED_FIELD = "id";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .module("test-esql-heap-attack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("esql.query.allow_partial_results", "false")
        // Allow setup to index a ~12MB source document; the bulk is routed through the small node.
        .setting("indexing_pressure.memory.primary.limit", "20%")
        .setting("indexing_pressure.memory.limit", "20%")
        .node(
            0,
            node -> node.setting("node.roles", "[master, data, ingest]")
                .jvmArg("-Xms" + DATA_NODE_HEAP_MB + "m -Xmx" + DATA_NODE_HEAP_MB + "m")
        )
        .node(1, node -> node.setting("node.roles", "[]").jvmArg("-Xms" + COORDINATOR_HEAP_MB + "m -Xmx" + COORDINATOR_HEAP_MB + "m"))
        .build();

    @Override
    protected String getTestRestCluster() {
        // Only the small coordinating-only node, so it is the one that flattens every query.
        return cluster.getHttpAddress(1);
    }

    @Before
    public void requireLoadAll() throws IOException {
        assumeTrue("requires the LOAD_ALL single column", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_SINGLE_SOURCE_COLUMN.isEnabled());
        assumeFalse("this suite needs its own two-node topology", isServerless());
    }

    /**
     * Index: one document whose {@code _source} is a single array of 1.5M tiny objects ({@code "a":[{"x":1},...]}), about 12MB.
     * <p>
     * Query: {@code LOAD_ALL} over it, from a 320MB coordinating-only node.
     * <p>
     * A {@code Map} of that array costs several hundred MB, more than the whole coordinator heap, while the leaves it flattens to are
     * a few tens of MB. The request must complete with the single {@code _unmapped_fields} column rather than take the node down.
     */
    public void testArrayOfTinyObjectsFlattensOnASmallCoordinator() throws IOException {
        String index = "load_all_array_small_coordinator";
        int elements = 1_500_000;
        CreateIndexResponse created = createIndex(index, Settings.builder().put("index.number_of_shards", 1).build(), """
            {
              "dynamic": false,
              "properties": {
                "%s": { "type": "keyword" }
              }
            }""".formatted(MAPPED_FIELD));
        assertTrue(created.isAcknowledged());

        StringBuilder doc = new StringBuilder(elements * 8 + 64);
        doc.append("{\"create\":{}}\n{\"").append(MAPPED_FIELD).append("\":\"0\",\"a\":[");
        for (int e = 0; e < elements; e++) {
            if (e > 0) {
                doc.append(',');
            }
            doc.append("{\"x\":1}");
        }
        doc.append("]}\n");
        bulk(index, doc.toString());
        refresh(index);

        StringBuilder query = startQuery();
        query.append("SET unmapped_fields=\\\"LOAD_ALL\\\";\n");
        query.append("FROM ").append(index).append("\n| LIMIT 1\"}");
        Response response = query(query.toString(), "columns");

        List<?> columns = (List<?>) responseAsMap(response).get("columns");
        List<String> names = new ArrayList<>(columns.size());
        for (Object column : columns) {
            names.add((String) ((Map<?, ?>) column).get("name"));
        }
        assertThat(names, containsInAnyOrder(MAPPED_FIELD, "_unmapped_fields"));
        assertThat(response.getWarnings(), empty());
    }

    private void bulk(String index, String body) throws IOException {
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(body);
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(5).millis())).build())
        );
        Response response = client().performRequest(request);
        assertEquals(false, entityAsMap(response).get("errors"));
    }
}
