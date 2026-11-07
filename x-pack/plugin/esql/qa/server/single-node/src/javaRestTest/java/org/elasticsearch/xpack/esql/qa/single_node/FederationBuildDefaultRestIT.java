/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end REST coverage for the default value of {@code esql.federation.enabled}: on for a stateful node whatever the
 * build, and for a stateless node only in a snapshot build; the only other thing that can take the feature away is a
 * Windows release build, where nothing can turn it on. The node type is read back from the node rather than assumed,
 * because this suite ships in a test artifact and may run against a cluster that {@link Clusters} did not define. Every
 * other suite here either pins the setting in {@code elasticsearch.yml} or unregisters the feature on the node JVM, which
 * decides the outcome before the default is ever consulted; the one exception is {@link FederationSettingRestartRestIT},
 * which takes the default for its first phase so that a dataset is actually read on a default-configured node.
 *
 * <p>The single test sends one request, the same one in every build and on every platform, and only the outcome
 * differs. Creating a data source is that request: it needs the whole feature to be available, and unlike creating a
 * dataset it stands alone, with no parent to create first that would make different configurations do different amounts
 * of work.
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

    public void testCreatingADataSourceFollowsTheDefault() throws IOException {
        Request putDataSource = new Request("PUT", "/_query/data_source/" + DATA_SOURCE);
        putDataSource.setJsonEntity("""
            {"type": "s3", "settings": {"region": "us-east-1", "auth": "anonymous"}}""");
        putDataSource.setOptions(putDataSource.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());

        boolean snapshot = Build.current().isSnapshot();
        boolean defaultOn = Federation.defaultEnabled(FederationNodeSettings.isStateless(client()), snapshot);
        boolean windowsRelease = Constants.WINDOWS && snapshot == false;
        if (defaultOn && windowsRelease == false) {
            // Federation is on by default on this node, so the data source is created.
            assertThat(client().performRequest(putDataSource).getStatusLine().getStatusCode(), equalTo(200));
            client().performRequest(new Request("DELETE", "/_query/data_source/" + DATA_SOURCE));
        } else {
            // Either the default is off (a stateless release build) or a Windows release build vetoes it. Either way the
            // node answers as if the feature never existed — the route is unregistered and the framework rejects the
            // request before any data source validation runs.
            ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(putDataSource));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("no handler found for uri"));
        }
    }
}
