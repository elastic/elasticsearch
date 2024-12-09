/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.junit.After;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public abstract class RequestIndexFilteringTestCase extends ESRestTestCase {

    @After
    public void wipeTestData() throws IOException {
        try {
            var response = client().performRequest(new Request("DELETE", "/test*"));
            assertEquals(200, response.getStatusLine().getStatusCode());
        } catch (ResponseException re) {
            assertEquals(404, re.getResponse().getStatusLine().getStatusCode());
        }
    }

    public void testTimestampFilterFromQuery() throws IOException {
        int docsTest1 = 50;
        int docsTest2 = 30;
        indexTimestampData(docsTest1, "test1", "2024-11-26", "id1");
        indexTimestampData(docsTest2, "test2", "2023-11-26", "id2");

        // filter includes both indices in the result (all columns, all rows)
        RestEsqlTestCase.RequestObjectBuilder builder = timestampFilter("gte", "2023-01-01").query("FROM test*");
        Map<String, Object> result = runEsql(builder);
        assertMap(
            result,
            matchesMap().entry(
                "columns",
                matchesList().item(matchesMap().entry("name", "@timestamp").entry("type", "date"))
                    .item(matchesMap().entry("name", "id1").entry("type", "integer"))
                    .item(matchesMap().entry("name", "id2").entry("type", "integer"))
                    .item(matchesMap().entry("name", "value").entry("type", "long"))
            ).entry("values", allOf(instanceOf(List.class), hasSize(docsTest1 + docsTest2))).entry("took", greaterThanOrEqualTo(0))
        );

        // filter includes only test1. Columns from test2 are filtered out, as well (not only rows)!
        builder = timestampFilter("gte", "2024-01-01").query("FROM test*");
        assertMap(
            runEsql(builder),
            matchesMap().entry(
                "columns",
                matchesList().item(matchesMap().entry("name", "@timestamp").entry("type", "date"))
                    .item(matchesMap().entry("name", "id1").entry("type", "integer"))
                    .item(matchesMap().entry("name", "value").entry("type", "long"))
            ).entry("values", allOf(instanceOf(List.class), hasSize(docsTest1))).entry("took", greaterThanOrEqualTo(0))
        );

        // filter excludes both indices (no rows); the first analysis step fails because there are no columns, a second attempt succeeds
        // after eliminating the index filter. All columns are returned.
        builder = timestampFilter("gte", "2025-01-01").query("FROM test*");
        assertMap(
            runEsql(builder),
            matchesMap().entry(
                "columns",
                matchesList().item(matchesMap().entry("name", "@timestamp").entry("type", "date"))
                    .item(matchesMap().entry("name", "id1").entry("type", "integer"))
                    .item(matchesMap().entry("name", "id2").entry("type", "integer"))
                    .item(matchesMap().entry("name", "value").entry("type", "long"))
            ).entry("values", allOf(instanceOf(List.class), hasSize(0))).entry("took", greaterThanOrEqualTo(0))
        );
    }

    public void testFieldExistsFilter_KeepWildcard() throws IOException {
        int docsTest1 = randomIntBetween(0, 10);
        int docsTest2 = randomIntBetween(0, 10);
        indexTimestampData(docsTest1, "test1", "2024-11-26", "id1");
        indexTimestampData(docsTest2, "test2", "2023-11-26", "id2");

        // filter includes only test1. Columns and rows of test2 are filtered out
        RestEsqlTestCase.RequestObjectBuilder builder = existsFilter("id1").query("FROM test*");
        Map<String, Object> result = runEsql(builder);
        assertMap(
            result,
            matchesMap().entry(
                "columns",
                matchesList().item(matchesMap().entry("name", "@timestamp").entry("type", "date"))
                    .item(matchesMap().entry("name", "id1").entry("type", "integer"))
                    .item(matchesMap().entry("name", "value").entry("type", "long"))
            ).entry("values", allOf(instanceOf(List.class), hasSize(docsTest1))).entry("took", greaterThanOrEqualTo(0))
        );

        // filter includes only test1. Columns from test2 are filtered out, as well (not only rows)!
        builder = existsFilter("id1").query("FROM test* METADATA _index | KEEP _index, id*");
        result = runEsql(builder);
        assertMap(
            result,
            matchesMap().entry(
                "columns",
                matchesList().item(matchesMap().entry("name", "_index").entry("type", "keyword"))
                    .item(matchesMap().entry("name", "id1").entry("type", "integer"))
            ).entry("values", allOf(instanceOf(List.class), hasSize(docsTest1))).entry("took", greaterThanOrEqualTo(0))
        );
        @SuppressWarnings("unchecked")
        var values = (List<List<Object>>) result.get("values");
        for (List<Object> row : values) {
            assertThat(row.get(0), equalTo("test1"));
            assertThat(row.get(1), instanceOf(Integer.class));
        }
    }

    public void testFieldExistsFilter_With_ExplicitUseOfDiscardedIndexFields() throws IOException {
        int docsTest1 = randomIntBetween(1, 5);
        int docsTest2 = randomIntBetween(0, 5);
        indexTimestampData(docsTest1, "test1", "2024-11-26", "id1");
        indexTimestampData(docsTest2, "test2", "2023-11-26", "id2");

        // test2 is explicitly used in a query with "SORT id2" even if the index filter should discard test2
        RestEsqlTestCase.RequestObjectBuilder builder = existsFilter("id1").query(
            "FROM test* METADATA _index | SORT id2 | KEEP _index, id*"
        );
        Map<String, Object> result = runEsql(builder);
        assertMap(
            result,
            matchesMap().entry(
                "columns",
                matchesList().item(matchesMap().entry("name", "_index").entry("type", "keyword"))
                    .item(matchesMap().entry("name", "id1").entry("type", "integer"))
                    .item(matchesMap().entry("name", "id2").entry("type", "integer"))
            ).entry("values", allOf(instanceOf(List.class), hasSize(docsTest1))).entry("took", greaterThanOrEqualTo(0))
        );
        @SuppressWarnings("unchecked")
        var values = (List<List<Object>>) result.get("values");
        for (List<Object> row : values) {
            assertThat(row.get(0), equalTo("test1"));
            assertThat(row.get(1), instanceOf(Integer.class));
            assertThat(row.get(2), nullValue());
        }
    }

    public void testFieldNameTypo() throws IOException {
        int docsTest1 = randomIntBetween(0, 5);
        int docsTest2 = randomIntBetween(0, 5);
        indexTimestampData(docsTest1, "test1", "2024-11-26", "id1");
        indexTimestampData(docsTest2, "test2", "2023-11-26", "id2");

        // idx field name is explicitly used, though it doesn't exist in any of the indices. First test - without filter
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> runEsql(requestObjectBuilder().query("FROM test* | WHERE idx == 123"))
        );
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));
        assertThat(e.getMessage(), containsString("Found 1 problem"));
        assertThat(e.getMessage(), containsString("line 1:20: Unknown column [idx]"));

        e = expectThrows(ResponseException.class, () -> runEsql(requestObjectBuilder().query("FROM test1 | WHERE idx == 123")));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));
        assertThat(e.getMessage(), containsString("Found 1 problem"));
        assertThat(e.getMessage(), containsString("line 1:20: Unknown column [idx]"));

        e = expectThrows(
            ResponseException.class,
            () -> runEsql(timestampFilter("gte", "2020-01-01").query("FROM test* | WHERE idx == 123"))
        );
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("Found 1 problem"));
        assertThat(e.getMessage(), containsString("line 1:20: Unknown column [idx]"));

        e = expectThrows(
            ResponseException.class,
            () -> runEsql(timestampFilter("gte", "2020-01-01").query("FROM test2 | WHERE idx == 123"))
        );
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("Found 1 problem"));
        assertThat(e.getMessage(), containsString("line 1:20: Unknown column [idx]"));
    }

    public void testIndicesDontExist() throws IOException {
        int docsTest1 = 0; // we are interested only in the created index, not necessarily that it has data
        indexTimestampData(docsTest1, "test1", "2024-11-26", "id1");

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(timestampFilter("gte", "2020-01-01").query("FROM foo")));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));
        assertThat(e.getMessage(), containsString("Unknown index [foo]"));

        e = expectThrows(ResponseException.class, () -> runEsql(timestampFilter("gte", "2020-01-01").query("FROM foo*")));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));
        assertThat(e.getMessage(), containsString("Unknown index [foo*]"));

        e = expectThrows(ResponseException.class, () -> runEsql(timestampFilter("gte", "2020-01-01").query("FROM foo,test1")));
        assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("index_not_found_exception"));
        assertThat(e.getMessage(), containsString("no such index [foo]"));
    }

    private static RestEsqlTestCase.RequestObjectBuilder timestampFilter(String op, String date) throws IOException {
        return requestObjectBuilder().filter(b -> {
            b.startObject("range");
            {
                b.startObject("@timestamp").field(op, date).endObject();
            }
            b.endObject();
        });
    }

    private static RestEsqlTestCase.RequestObjectBuilder existsFilter(String field) throws IOException {
        return requestObjectBuilder().filter(b -> b.startObject("exists").field("field", field).endObject());
    }

    public Map<String, Object> runEsql(RestEsqlTestCase.RequestObjectBuilder requestObject) throws IOException {
        return RestEsqlTestCase.runEsql(requestObject, new AssertWarnings.NoWarnings(), RestEsqlTestCase.Mode.SYNC);
    }

    protected void indexTimestampData(int docs, String indexName, String date, String differentiatorFieldName) throws IOException {
        Request createIndex = new Request("PUT", indexName);
        createIndex.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 3
                }
              },
              "mappings": {
                "properties": {
                  "@timestamp": {
                    "type": "date"
                  },
                  "value": {
                    "type": "long"
                  },
                  "%differentiator_field_name%": {
                    "type": "integer"
                  }
                }
              }
            }""".replace("%differentiator_field_name%", differentiatorFieldName));
        Response response = client().performRequest(createIndex);
        assertThat(
            entityToMap(response.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", indexName).entry("acknowledged", true)
        );

        if (docs > 0) {
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < docs; i++) {
                b.append(String.format(Locale.ROOT, """
                    {"create":{"_index":"%s"}}
                    {"@timestamp":"%s","value":%d,"%s":%d}
                    """, indexName, date, i, differentiatorFieldName, i));
            }
            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.addParameter("filter_path", "errors");
            bulk.setJsonEntity(b.toString());
            response = client().performRequest(bulk);
            Assert.assertEquals("{\"errors\":false}", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        }
    }
}
