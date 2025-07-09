/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.commonProfile;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.fixTypesOnProfile;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@code index.esql.stored_fields_sequential_proportion} which controls
 * an optimization we use when loading from {@code _source}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class StoredFieldsSequentialIT extends ESRestTestCase {
    private static final Logger LOG = LogManager.getLogger(StoredFieldsSequentialIT.class);

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    public void testFetchTen() throws IOException {
        testQuery(null, """
            FROM test
            | LIMIT 10
            """, 10, true);
    }

    public void testAggAll() throws IOException {
        testQuery(null, """
            FROM test
            | STATS SUM(LENGTH(test))
            """, 1000, true);
    }

    public void testAggTwentyPercent() throws IOException {
        testQuery(null, """
            FROM test
            | WHERE STARTS_WITH(test.keyword, "test1") OR STARTS_WITH(test.keyword, "test2")
            | STATS SUM(LENGTH(test))
            """, 200, true);
    }

    public void testAggTenPercentDefault() throws IOException {
        testAggTenPercent(null, false);
    }

    public void testAggTenPercentConfiguredToTenPct() throws IOException {
        testAggTenPercent(0.10, true);
    }

    public void testAggTenPercentConfiguredToOnePct() throws IOException {
        testAggTenPercent(0.01, true);
    }

    /**
     * It's important for the test that the queries we use detect "scattered" docs.
     * If they were "compact" in the index we'd still load them using the sequential
     * reader.
     */
    private void testAggTenPercent(Double percent, boolean sequential) throws IOException {
        String filter = IntStream.range(0, 10)
            .mapToObj(i -> String.format(Locale.ROOT, "STARTS_WITH(test.keyword, \"test%s%s\")", i, i))
            .collect(Collectors.joining(" OR "));
        testQuery(percent, String.format(Locale.ROOT, """
            FROM test
            | WHERE %s
            | STATS SUM(LENGTH(test))
            """, filter), 100, sequential);
    }

    private void testQuery(Double percent, String query, int documentsFound, boolean sequential) throws IOException {
        setPercent(percent);
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(query);
        builder.profile(true);
        Map<String, Object> result = runEsql(builder, new AssertWarnings.NoWarnings(), RestEsqlTestCase.Mode.SYNC);
        assertMap(
            result,
            matchesMap().entry("documents_found", documentsFound)
                .entry(
                    "profile",
                    matchesMap() //
                        .entry("drivers", instanceOf(List.class))
                        .entry("plans", instanceOf(List.class))
                        .entry("planning", matchesMap().extraOk())
                        .entry("query", matchesMap().extraOk())
                )
                .extraOk()
        );

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");
        for (Map<String, Object> p : profiles) {
            fixTypesOnProfile(p);
            assertThat(p, commonProfile());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> operators = (List<Map<String, Object>>) p.get("operators");
            for (Map<String, Object> o : operators) {
                LOG.info("profile {}", o.get("operator"));
            }
            for (Map<String, Object> o : operators) {
                checkOperatorProfile(o, sequential);
            }
        }
    }

    private void setPercent(Double percent) throws IOException {
        Request set = new Request("PUT", "test/_settings");
        set.setJsonEntity(String.format(Locale.ROOT, """
            {
                "index": {
                    "esql": {
                        "stored_fields_sequential_proportion": %s
                    }
                }
            }
            """, percent));
        assertMap(entityToMap(client().performRequest(set).getEntity(), XContentType.JSON), matchesMap().entry("acknowledged", true));
    }

    @Before
    public void buildIndex() throws IOException {
        Request exists = new Request("GET", "test");
        try {
            client().performRequest(exists);
            return;
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
        Request createIndex = new Request("PUT", "test");
        createIndex.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "sort.field": "i"
                }
              },
              "mappings": {
                "properties": {
                  "i": {"type": "long"}
                }
              }
            }""");
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "test").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            b.append(String.format(Locale.ROOT, """
                {"create":{"_index":"test"}}
                {"test":"test%03d", "i": %d}
                """, i, i));
        }
        bulk.setJsonEntity(b.toString());
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    private static void checkOperatorProfile(Map<String, Object> o, boolean sequential) {
        String name = (String) o.get("operator");
        if (name.startsWith("ValuesSourceReaderOperator")) {
            MapMatcher readersBuilt = matchesMap().entry(
                "stored_fields[requires_source:true, fields:0, sequential: " + sequential + "]",
                greaterThanOrEqualTo(1)
            ).extraOk();
            MapMatcher expectedOp = matchesMap().entry("operator", name)
                .entry("status", matchesMap().entry("readers_built", readersBuilt).extraOk());
            assertMap(o, expectedOp);
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
