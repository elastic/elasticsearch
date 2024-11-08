/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class RestEnrichTestCase extends ESRestTestCase {

    private static final String sourceIndexName = "countries";
    private static final String testIndexName = "test";
    private static final String policyName = "countries";

    public enum Mode {
        SYNC,
        ASYNC
    }

    protected final Mode mode;

    @ParametersFactory
    public static List<Object[]> modes() {
        return Arrays.stream(Mode.values()).map(m -> new Object[] { m }).toList();
    }

    protected RestEnrichTestCase(Mode mode) {
        this.mode = mode;
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    @Before
    public void loadTestData() throws IOException {
        Request request = new Request("PUT", "/" + testIndexName);
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "geo.dest": {
                    "type": "keyword"
                  },
                  "number": {
                    "type": "long"
                  }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/" + testIndexName + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            { "index": {"_id": 1} }
            { "geo.dest": "US", "number": 1000 }
            { "index": {"_id": 2} }
            { "geo.dest": "US", "number": 1000 }
            { "index": {"_id": 3} }
            { "geo.dest": "CN", "number": 5000 }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("PUT", "/" + sourceIndexName);
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "geo.dest": {
                    "type": "keyword"
                  },
                  "country_name": {
                    "type": "keyword"
                  }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/" + sourceIndexName + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            { "index" : {}}
            { "geo.dest": "US", "country_name": "United States of America" }
            { "index" : {}}
            { "geo.dest": "IN", "country_name": "India" }
            { "index" : {}}
            { "geo.dest": "CN", "country_name": "China" }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("PUT", "/_enrich/policy/" + policyName);
        request.setJsonEntity("""
            {
              "match": {
                "indices": "countries",
                "match_field": "geo.dest",
                "enrich_fields": ["country_name"]
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("PUT", "/_enrich/policy/" + policyName + "/_execute");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    @After
    public void wipeTestData() throws IOException {
        try {
            var response = client().performRequest(new Request("DELETE", "/" + testIndexName));
            assertEquals(200, response.getStatusLine().getStatusCode());
            response = client().performRequest(new Request("DELETE", "/" + sourceIndexName));
            assertEquals(200, response.getStatusLine().getStatusCode());
            response = client().performRequest(new Request("DELETE", "/_enrich/policy/" + policyName));
            assertEquals(200, response.getStatusLine().getStatusCode());
        } catch (ResponseException re) {
            assertEquals(404, re.getResponse().getStatusLine().getStatusCode());
        }
    }

    public void testNonExistentEnrichPolicy() throws IOException {
        ResponseException re = expectThrows(ResponseException.class, () -> runEsql("from test | enrich countris", Mode.SYNC));
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("cannot find enrich policy [countris], did you mean [countries]?")
        );
    }

    public void testNonExistentEnrichPolicy_KeepField() throws IOException {
        ResponseException re = expectThrows(ResponseException.class, () -> runEsql("from test | enrich countris | keep number", Mode.SYNC));
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("cannot find enrich policy [countris], did you mean [countries]?")
        );
    }

    public void testMatchField_ImplicitFieldsList() throws IOException {
        Map<String, Object> result = runEsql("from test | enrich countries | keep number | sort number");
        var columns = List.of(Map.of("name", "number", "type", "long"));
        var values = List.of(List.of(1000), List.of(1000), List.of(5000));
        assertMap(result, matchesMap().entry("columns", columns).entry("values", values).entry("took", greaterThanOrEqualTo(0)));
    }

    public void testMatchField_ImplicitFieldsList_WithStats() throws IOException {
        Map<String, Object> result = runEsql("from test | enrich countries | stats s = sum(number) by country_name");
        var columns = List.of(Map.of("name", "s", "type", "long"), Map.of("name", "country_name", "type", "keyword"));
        var values = List.of(List.of(2000, "United States of America"), List.of(5000, "China"));
        assertMap(result, matchesMap().entry("columns", columns).entry("values", values).entry("took", greaterThanOrEqualTo(0)));
    }

    private Map<String, Object> runEsql(String query) throws IOException {
        return runEsql(query, mode);
    }

    private Map<String, Object> runEsql(String query, Mode mode) throws IOException {
        var requestObject = new RestEsqlTestCase.RequestObjectBuilder().query(query);
        if (mode == Mode.ASYNC) {
            return RestEsqlTestCase.runEsqlAsync(requestObject);
        } else {
            return RestEsqlTestCase.runEsqlSync(requestObject);
        }
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }
}
