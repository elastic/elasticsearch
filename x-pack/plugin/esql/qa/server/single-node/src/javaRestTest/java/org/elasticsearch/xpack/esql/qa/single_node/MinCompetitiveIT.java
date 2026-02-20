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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.signature;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MinCompetitiveIT extends ESRestTestCase {
    private static final long DATE_ROOT = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2026-01-01T00:00:00Z");
    private static final int BATCHES = 10;
    private static final int PER_BATCH = 10000;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        c -> c.setting("logger.org.elasticsearch.compute.lucene.query", "TRACE")
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testTimestampAsc() throws IOException {
        List<ListMatcher> values = new ArrayList<>();
        int n = 0;
        while (values.size() < 10) {
            if (n % 10 < 8) {
                values.add(matchesList().item(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DATE_ROOT + n * 1000L)).item(n));
            }
            n++;
        }
        run("""
            FROM test
            | WHERE n % 10 < 8
            | SORT @timestamp ASC
            | LIMIT 10
            """, matchesList(values));
    }

    public void testTimestampDesc() throws IOException {
        List<ListMatcher> values = new ArrayList<>();
        int n = BATCHES * PER_BATCH - 1;
        while (values.size() < 10) {
            if (n % 10 < 8) {
                values.add(matchesList().item(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DATE_ROOT + n * 1000L)).item(n));
            }
            n--;
        }
        run("""
            FROM test
            | WHERE n % 10 < 8
            | SORT @timestamp DESC
            | LIMIT 10
            """, matchesList(values));
    }

    public void testNAscNullsFirstTimestampAsc() throws IOException {
        List<ListMatcher> values = new ArrayList<>();
        int n = 0;
        while (values.size() < 10) {
            if (n % 10 == 9) {
                values.add(matchesList().item(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DATE_ROOT + n * 1000L)).item(null));
            }
            n++;
        }
        run("""
            FROM test
            | WHERE n % 10 < 8 OR n IS NULL
            | SORT n ASC NULLS FIRST, @timestamp ASC
            | LIMIT 10
            """, matchesList(values));
    }

    public void testNAscNullsLastTimestampAsc() throws IOException {
        List<ListMatcher> values = new ArrayList<>();
        int n = 0;
        while (values.size() < 10) {
            if (n % 10 < 8) {
                values.add(matchesList().item(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DATE_ROOT + n * 1000L)).item(n));
            }
            n++;
        }
        run("""
            FROM test
            | WHERE n % 10 < 8 OR n IS NULL
            | SORT n ASC NULLS LAST, @timestamp ASC
            | LIMIT 10
            """, matchesList(values));
    }

    public void testNDescNullsFirstTimestampAsc() throws IOException {
        List<ListMatcher> values = new ArrayList<>();
        int n = 0;
        while (values.size() < 10) {
            if (n % 10 == 9) {
                values.add(matchesList().item(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DATE_ROOT + n * 1000L)).item(null));
            }
            n++;
        }
        run("""
            FROM test
            | WHERE n % 10 < 8 OR n IS NULL
            | SORT n DESC NULLS FIRST, @timestamp ASC
            | LIMIT 10
            """, matchesList(values));
    }

    public void testNDescNullsLastTimestampAsc() throws IOException {
        List<ListMatcher> values = new ArrayList<>();
        int n = BATCHES * PER_BATCH - 1;
        while (values.size() < 10) {
            if (n % 10 < 8) {
                values.add(matchesList().item(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DATE_ROOT + n * 1000L)).item(n));
            }
            n--;
        }
        run("""
            FROM test
            | WHERE n % 10 < 8 OR n IS NULL
            | SORT n DESC NULLS LAST, @timestamp ASC
            | LIMIT 10
            """, matchesList(values));
    }

    private void run(String query, ListMatcher valuesMatcher) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(query);
        builder.pragmasOk()
            .pragmas(
                Settings.builder()
                    /*
                     * Force small pages and many threads so we get a few updates,
                     * even if the index is sorted.
                     */
                    .put(QueryPragmas.PAGE_SIZE.getKey(), 100)
                    .put(QueryPragmas.DATA_PARTITIONING.getKey(), "doc")
                    .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 5)
                    .build()
            );
        builder.profile(true);
        Map<String, Object> result = runEsql(builder, new AssertWarnings.NoWarnings(), null, RestEsqlTestCase.Mode.SYNC);

        ListMatcher columns = matchesList().item(matchesMap().entry("name", "@timestamp").entry("type", "date"))
            .item(matchesMap().entry("name", "n").entry("type", "long"));

        MapMatcher matcher = matchesMap().extraOk();
        // If this is equalTo then we didn't use min_competitive
        matcher = matcher.entry("documents_found", lessThan(BATCHES * PER_BATCH));
        matcher = matcher.entry("columns", columns);
        matcher = matcher.entry("values", valuesMatcher);
        assertMap(result, matcher);

        // Check the profile for min_competitive
        Map<?, ?> profile = (Map<?, ?>) result.get("profile");
        List<?> drivers = (List<?>) profile.get("drivers");
        boolean foundData = false;
        for (Object d : drivers) {
            Map<?, ?> driver = (Map<?, ?>) d;
            String description = (String) driver.get("description");
            if ("data".equals(description) == false) {
                continue;
            }
            foundData = true;
            List<?> operators = (List<?>) driver.get("operators");
            boolean foundSource = false;
            boolean foundTopN = false;
            for (Object o : operators) {
                @SuppressWarnings("unchecked")
                Map<String, Object> operator = (Map<String, Object>) o;
                Map<?, ?> status = (Map<?, ?>) operator.get("status");
                String name = signature(operator);
                switch (name) {
                    case "LuceneSourceOperator" -> {
                        foundSource = true;
                        MapMatcher minCompetitive = matchesMap().entry("match_all", greaterThanOrEqualTo(0))
                            .entry("match_none", greaterThanOrEqualTo(0))
                            .entry("greater_than_min_competitive", greaterThanOrEqualTo(0))
                            .entry("changed_value", greaterThanOrEqualTo(0))
                            .entry("update_nanos", greaterThanOrEqualTo(0));
                        assertThat(status, matchesMap().extraOk().entry("min_competitive", minCompetitive));
                    }
                    case "TopNOperator" -> {
                        foundTopN = true;
                        // NOCOMMIT check min_competitive_updates
                    }
                }
            }
            assertThat(foundSource, equalTo(true));
            assertThat(foundTopN, equalTo(true));
        }
        assertThat(foundData, equalTo(true));
    }

    @Before
    public void createIndex() throws IOException {
        if (indexExists("test")) {
            return;
        }

        Request create = new Request("PUT", "/test");
        create.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "number_of_replicas": 0
                }
              }
            }""");
        Response createResponse = client().performRequest(create);
        assertThat(entityToMap(createResponse.getEntity(), XContentType.JSON), matchesMap().entry("acknowledged", true).extraOk());

        Request bulk = new Request("POST", "/test/_bulk");
        bulk.addParameter("refresh", "");
        StringBuilder b = new StringBuilder();
        int n = 0;
        for (int batch = 0; batch < BATCHES; batch++) {
            for (int i = 0; i < PER_BATCH; i++) {
                String ts = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DATE_ROOT + n * 1000L);
                b.append("{\"create\":{}}\n");
                if (i % 10 == 9) {
                    b.append(String.format(Locale.ROOT, "{\"@timestamp\": \"%s\"}\n", ts));
                } else {
                    b.append(String.format(Locale.ROOT, "{\"@timestamp\": \"%s\", \"n\": %s}\n", ts, n));
                }
                n++;
            }
            bulk.setJsonEntity(b.toString());
            b.setLength(0);
            Response bulkResponse = client().performRequest(bulk);
            assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
        }

        Request refresh = new Request("POST", "/test/_refresh");
        Response refreshResponse = client().performRequest(refresh);
        assertThat(
            entityToMap(refreshResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("_shards", matchesMap().entry("successful", 1).extraOk())
        );
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }
}
