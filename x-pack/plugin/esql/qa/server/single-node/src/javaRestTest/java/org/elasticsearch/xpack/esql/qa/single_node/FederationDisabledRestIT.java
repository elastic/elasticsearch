/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end REST coverage for a node that boots with the federation kill switch already engaged
 * ({@code -Des.esql.federation.enabled=false}), the always-off deployment shape. Proves the create
 * paths are dead (403) while GET still works. The switch is read once at node startup, so this needs
 * a dedicated cluster with the system property set on the node JVM (see the {@code @ClassRule}).
 *
 * <p>Behavior against <em>pre-existing</em> federation state (creating a dataset on an existing data
 * source, executing {@code FROM <dataset>}, and DELETE + GET cleanup) cannot be exercised here,
 * because a boot-disabled node cannot create that state; it is covered by
 * {@link FederationKillSwitchRestartRestIT}, which creates state while enabled and then restarts the
 * node with the switch off. The complementary enabled-path CRUD coverage lives in
 * {@link DataSourceCrudRestIT}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationDisabledRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(spec -> spec.systemProperty(Federation.ENABLED_PROPERTY, "false"));

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    public void testPutDataSourceIsForbidden() throws IOException {
        Request req = new Request("PUT", "/_query/data_source/blocked_ds");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("type", "s3").field("settings", Map.of("region", "us-east-1", "auth", "anonymous")).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(req));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString(Federation.ENABLED_PROPERTY));
    }

    public void testPutDatasetIsForbidden() throws IOException {
        Request req = new Request("PUT", "/_query/dataset/blocked_dataset");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", "some_parent").field("resource", "s3://bucket/x/*.parquet").endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(req));
        // The kill switch must fire before parent-existence validation, so this is a 403, not the 404 a missing parent
        // would produce on an enabled node.
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString(Federation.ENABLED_PROPERTY));
    }

    public void testGetDataSourceIsAllowed() throws IOException {
        Response resp = client().performRequest(new Request("GET", "/_query/data_source"));
        assertThat(resp.getStatusLine().getStatusCode(), equalTo(200));
    }

    public void testGetDatasetIsAllowed() throws IOException {
        Response resp = client().performRequest(new Request("GET", "/_query/dataset"));
        assertThat(resp.getStatusLine().getStatusCode(), equalTo(200));
    }
}
