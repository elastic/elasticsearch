/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.hamcrest.Matchers.containsString;

public class RestEnrichTestCase extends ESRestTestCase {

    private static final String sourceIndexName = "countries";
    private static final String testIndexName = "test";
    private static final String policyName = "countries";

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
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RestEsqlTestCase.RequestObjectBuilder().query("from test | enrich countris").build())
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("unresolved enrich policy [countris], did you mean [countries]?")
        );
    }

    public void testNonExistentEnrichPolicy_KeepField() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql(new RestEsqlTestCase.RequestObjectBuilder().query("from test | enrich countris | keep number").build())
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("unresolved enrich policy [countris], did you mean [countries]?")
        );
    }

    public void testMatchField_ImplicitFieldsList() throws IOException {
        Map<String, Object> result = runEsql(
            new RestEsqlTestCase.RequestObjectBuilder().query("from test | enrich countries | keep number").build()
        );
        var columns = List.of(Map.of("name", "number", "type", "long"));
        var values = List.of(List.of(1000), List.of(1000), List.of(5000));

        assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
    }

    public void testMatchField_ImplicitFieldsList_WithStats() throws IOException {
        Map<String, Object> result = runEsql(
            new RestEsqlTestCase.RequestObjectBuilder().query("from test | enrich countries | stats s = sum(number) by country_name")
                .build()
        );
        var columns = List.of(Map.of("name", "s", "type", "long"), Map.of("name", "country_name", "type", "keyword"));
        var values = List.of(List.of(2000, "United States of America"), List.of(5000, "China"));

        assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }
}
