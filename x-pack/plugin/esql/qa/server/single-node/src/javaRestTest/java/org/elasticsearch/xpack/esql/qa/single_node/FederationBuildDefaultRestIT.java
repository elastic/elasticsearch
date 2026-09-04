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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end REST coverage for the default value of {@code esql.federation.enabled}, which follows the build: on in a
 * snapshot build, off in a release build. This is the only suite here that takes that default, so it is the only one
 * whose result changes with the build. Every other suite either pins the setting in {@code elasticsearch.yml} or
 * unregisters the feature on the node JVM, which decides the outcome before the default is ever consulted.
 *
 * <p>The single test sends one request, the same one in either build, and only the outcome differs. Creating a data
 * source is that request: it needs the whole feature to be available, and unlike creating a dataset it stands alone,
 * with no parent to create first that would make the two builds do different amounts of work.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationBuildDefaultRestIT extends ESRestTestCase {

    private static final String DATA_SOURCE = "build_default_ds";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.clusterWithoutFederationSettings();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCreatingADataSourceFollowsTheBuildDefault() throws IOException {
        Request putDataSource = new Request("PUT", "/_query/data_source/" + DATA_SOURCE);
        putDataSource.setJsonEntity("""
            {"type": "s3", "settings": {"region": "us-east-1", "auth": "anonymous"}}""");

        if (Build.current().isSnapshot()) {
            // Federation is on, so the data source is created.
            assertThat(client().performRequest(putDataSource).getStatusLine().getStatusCode(), equalTo(200));
            client().performRequest(new Request("DELETE", "/_query/data_source/" + DATA_SOURCE));
        } else {
            // Federation is off, so the node answers as if it never shipped the feature: the route is unregistered,
            // and the framework rejects the request before any data source validation runs.
            ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(putDataSource));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("no handler found for uri"));
        }
    }
}
