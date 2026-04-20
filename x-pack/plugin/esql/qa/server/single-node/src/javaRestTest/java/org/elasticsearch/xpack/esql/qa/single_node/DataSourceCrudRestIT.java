/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end REST coverage for the ES|QL data source + dataset CRUD API against a running
 * cluster with the {@code esql-datasource-s3} plugin loaded. Exercises lifecycle, secret
 * masking in responses, and validator rejection — scenarios that the in-JVM
 * DataSourceCrudIT cannot express over HTTP and that the plugin-free YAML tests cannot
 * exercise because they lack a loaded validator.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DataSourceCrudRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testLifecycle() throws IOException {
        final String name = "lifecycle_ds";

        putDataSource(name, "s3", Map.of("region", "us-east-1"));

        Map<String, Object> got = getDataSource(name);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) got.get("data_sources");
        assertThat(hits, hasSize(1));
        assertThat(hits.get(0).get("name"), equalTo(name));
        assertThat(hits.get(0).get("type"), equalTo("s3"));

        deleteDataSource(name);
        ResponseException missing = expectThrows(ResponseException.class, () -> getDataSource(name));
        assertThat(missing.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testSecretMaskedInResponse() throws IOException {
        final String name = "secret_rt";
        putDataSource(name, "s3", Map.of("region", "us-west-2", "access_key", "AKIAFAKE", "secret_key", "SECRETVALUE"));

        Map<String, Object> got = getDataSource(name);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) got.get("data_sources");
        assertThat(hits, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> settings = (Map<String, Object>) hits.get(0).get("settings");
        assertThat("region round-trips plain", settings.get("region"), equalTo("us-west-2"));
        assertThat("access_key must be masked", settings.get("access_key").toString(), not(containsString("AKIAFAKE")));
        assertThat("secret_key must be masked", settings.get("secret_key").toString(), not(containsString("SECRETVALUE")));

        deleteDataSource(name);
    }

    public void testValidatorRejectsUnknownType() throws IOException {
        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> putDataSource("bad_type", "definitely_not_a_real_type", Map.of())
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("unknown data source type"));
    }

    private static Map<String, Object> getDataSource(String name) throws IOException {
        Response resp = client().performRequest(new Request("GET", "/_query/data_source/" + name));
        return entityAsMap(resp);
    }

    private static void putDataSource(String name, String type, Map<String, Object> settings) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("type", type);
            if (settings.isEmpty() == false) {
                b.field("settings", settings);
            }
            b.endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void deleteDataSource(String name) throws IOException {
        Response r = client().performRequest(new Request("DELETE", "/_query/data_source/" + name));
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }
}
