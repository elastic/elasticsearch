/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

/**
 * A key of a {@code flattened} field is accelerated on the shard like a {@code keyword} field: each query runs the same operators on
 * the data node over an index that keeps {@code attrs} as a {@code flattened} field as over one that maps {@code attrs.bar} as a
 * {@code keyword}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushFlattenedKeysIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @Before
    public void createIndices() throws IOException {
        assumeTrue(
            "keys of flattened fields are fields",
            clusterHasCapability("POST", "/_query", List.of(), List.of("flattened_subfields")).orElse(false)
        );
        createIndex("keys", """
            { "properties": { "attrs": { "type": "flattened" } } }""");
        createIndex("keywords", """
            { "properties": { "attrs": { "properties": { "bar": { "type": "keyword" } } } } }""");
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        StringBuilder body = new StringBuilder();
        for (String index : List.of("keys", "keywords")) {
            for (String value : List.of("a", "b", "c")) {
                body.append("{\"create\":{\"_index\":\"").append(index).append("\"}}\n");
                body.append("{\"attrs\":{\"bar\":\"").append(value).append("\"}}\n");
            }
        }
        bulk.setJsonEntity(body.toString());
        client().performRequest(bulk);
    }

    private static void createIndex(String name, String mapping) throws IOException {
        Request request = new Request("PUT", name);
        request.setJsonEntity("{ \"settings\": { \"index\": { \"number_of_shards\": 1 } }, \"mappings\": " + mapping + " }");
        client().performRequest(request);
    }

    public void testFilter() throws IOException {
        assertSameAsKeyword("| WHERE attrs.bar == \"a\" | KEEP attrs.bar");
    }

    public void testPattern() throws IOException {
        assertSameAsKeyword("| WHERE attrs.bar LIKE \"a*\" OR attrs.bar RLIKE \"b.*\" | KEEP attrs.bar");
    }

    public void testExists() throws IOException {
        assertSameAsKeyword("| WHERE attrs.bar IS NOT NULL | STATS c = COUNT(*)");
    }

    public void testCountOfFilter() throws IOException {
        assertSameAsKeyword("| WHERE attrs.bar == \"a\" | STATS c = COUNT(*)");
    }

    public void testTopN() throws IOException {
        assertSameAsKeyword("| SORT attrs.bar DESC | LIMIT 2 | KEEP attrs.bar");
    }

    public void testGroupBy() throws IOException {
        assertSameAsKeyword("| STATS c = COUNT(*) BY attrs.bar");
    }

    private void assertSameAsKeyword(String query) throws IOException {
        List<String> keys = dataNodeOperators("FROM keys " + query);
        assertThat(keys, hasItem(startsWith("Lucene")));
        assertThat(keys, equalTo(dataNodeOperators("FROM keywords " + query)));
    }

    /** The operators of the drivers that read the index, by name. */
    private List<String> dataNodeOperators(String query) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(query + "\n| LIMIT 100");
        builder.profile(true);
        Map<String, Object> result = runEsql(builder, new AssertWarnings.NoWarnings(), profileLogger, RestEsqlTestCase.Mode.SYNC);
        List<String> operators = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> drivers = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");
        for (Map<String, Object> driver : drivers) {
            if ("data".equals(driver.get("description"))) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> ops = (List<Map<String, Object>>) driver.get("operators");
                for (Map<String, Object> op : ops) {
                    operators.add(((String) op.get("operator")).replaceAll("\\[.*", ""));
                }
            }
        }
        return operators;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
