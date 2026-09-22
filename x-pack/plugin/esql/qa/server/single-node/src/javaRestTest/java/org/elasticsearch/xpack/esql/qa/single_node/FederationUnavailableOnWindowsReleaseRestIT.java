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
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeTrue;

/**
 * End-to-end proof that external data sources cannot be reached on a Windows release build even when explicitly
 * enabled.
 *
 * <p>The cluster pins {@code esql.federation.enabled} to {@code true} — so this suite proves the stronger claim:
 * a node <em>explicitly asked</em> for the feature still does not get it. The setting is accepted (the node starts
 * normally), but {@link org.elasticsearch.xpack.esql.datasources.Federation#isAvailable} returns {@code false} and the
 * feature's whole REST surface stays absent. {@link #testEnabledSettingIsIgnoredOnWindowsRelease()} pins both halves
 * of that claim; the six inherited route assertions verify the REST surface independently.
 *
 * <p>Windows snapshot builds are deliberately excluded: the feature works there, which is what lets the ES|QL
 * federation suites keep running on Windows in CI.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationUnavailableOnWindowsReleaseRestIT extends AbstractFederationUnavailableRestTestCase {

    private static final ElasticsearchCluster cluster = Clusters.testCluster();

    /**
     * The assumption has to run <em>before</em> the cluster rule, not in {@code @BeforeClass}: JUnit applies class
     * rules around the before-class methods, so an assumption there would boot a node on every other platform and
     * only then skip. Chaining it outside the cluster skips the suite without starting anything.
     */
    @ClassRule
    public static TestRule windowsReleaseOnlyCluster = RuleChain.outerRule((base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeTrue(
                "external data sources cannot be enabled on a Windows release build",
                Constants.WINDOWS && Build.current().isSnapshot() == false
            );
            base.evaluate();
        }
    }).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Verifies the complete chain: the node was configured with {@code esql.federation.enabled=true}, accepted the
     * value without error, yet the feature is still absent — the data source route returns the standard
     * {@code no handler found for uri} that an unregistered endpoint gives.
     */
    public void testEnabledSettingIsIgnoredOnWindowsRelease() throws IOException {
        Request nodeSettings = new Request("GET", "/_nodes/settings");
        nodeSettings.addParameter("filter_path", "nodes.*.settings.esql.federation.enabled");
        String settingsBody = EntityUtils.toString(client().performRequest(nodeSettings).getEntity());
        assertThat("the node was configured with federation enabled", settingsBody, containsString("true"));

        Request getDs = new Request("GET", "/_query/data_source");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(getDs));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("no handler found for uri"));
    }
}
