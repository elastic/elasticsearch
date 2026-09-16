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
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assume.assumeTrue;

/**
 * End-to-end proof that external data sources cannot be reached on a Windows release build, which is the platform
 * they are not shipped for.
 *
 * <p>The cluster is the ordinary {@link Clusters#testCluster()}, which pins {@code esql.federation.enabled} to
 * {@code true} — so this suite is not asserting that an unconfigured node has the feature off, it is asserting that a
 * node explicitly <em>asked</em> for it still does not get it. That is the distinction the platform gate exists to
 * make: the setting is accepted rather than rejected, so the node starts normally and logs why it is being ignored
 * (see {@link Federation#logEffectiveState}), but {@link Federation#isAvailable} stays {@code false} and the feature's
 * whole REST surface never appears.
 *
 * <p>All of the assertions are inherited: the six data source and dataset routes answer
 * {@code no handler found for uri}, and {@code FROM <dataset>} reads as a plain missing index. Those are the same
 * assertions used for a node whose operator removed the feature, which is the point — the two are indistinguishable
 * from outside.
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
                "external data sources are only unreachable-by-configuration on a Windows release build",
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
     * Pins the premise the rest of this suite rests on: the node really was told to enable federation. Without this
     * the inherited assertions would also pass on a node that simply never asked for the feature, which is a much
     * weaker claim than the one being made here.
     *
     * <p>The setting is <em>accepted</em> — it reads back as configured — and still decides nothing.
     */
    public void testNodeWasAskedToEnableFederation() throws IOException {
        Request settings = new Request("GET", "/_nodes/settings");
        settings.addParameter("filter_path", "nodes.*.settings.esql.federation.enabled");
        String body = EntityUtils.toString(client().performRequest(settings).getEntity());
        assertThat("the cluster must pin the gate on for this suite to mean anything", body, containsString("true"));
    }
}
