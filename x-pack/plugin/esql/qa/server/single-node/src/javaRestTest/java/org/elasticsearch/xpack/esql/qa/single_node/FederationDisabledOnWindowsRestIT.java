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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeFalse;

/**
 * End-to-end REST coverage for a node that was given no federation configuration at all, on the platform where that
 * means the feature is off: Windows, where {@link Federation#REGISTER_PROPERTY} defaults to {@code false} so that a
 * node behaves out of the box like the release build it ships as.
 *
 * <p>What separates this suite from its siblings is the cluster: {@link Clusters#clusterWithoutFederationSettings()}
 * configures <em>nothing</em> — no property override and no {@code esql.federation.enabled}.
 * {@link FederationDisabledRestIT} proves the feature is off when an operator turns it off; this proves it is off
 * when nobody touches it, which is the actual platform requirement and is not reachable through either lever.
 *
 * <p>{@link FederationBuildDefaultRestIT} takes the same cluster and asserts the opposite outcome, because its
 * subject is the {@code esql.federation.enabled} build default. The two do not conflict: registration is resolved
 * first, so where it defaults off the build default is never consulted, and that suite skips exactly where this one
 * runs.
 *
 * <p>Note this covers both builds. A Windows release build additionally cannot be made to turn the feature on at all
 * ({@link Federation#SUPPORTED} is false and the property is rejected); a snapshot build can, which is how the
 * federation suites still run on Windows in CI — they ask for it explicitly through {@code Clusters}.
 *
 * <p>The inherited assertions cover the unavailable surface (the six data source and dataset routes, and
 * {@code FROM <dataset>} reading as a missing index); the two below add the paths specific to this gate.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationDisabledOnWindowsRestIT extends AbstractFederationUnavailableRestTestCase {

    private static final ElasticsearchCluster cluster = Clusters.clusterWithoutFederationSettings();

    /**
     * The assumption has to run <em>before</em> the cluster rule, not in {@code @BeforeClass}: JUnit applies class
     * rules around the before-class methods, so an assumption there would boot a node on every other platform and
     * only then skip. Chaining it outside the cluster skips the suite without starting anything.
     */
    @ClassRule
    public static TestRule clusterWhereRegistrationDefaultsOff = RuleChain.outerRule((base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("covers the platform where federation is off unless asked for", Federation.DEFAULT_REGISTERED);
            base.evaluate();
        }
    }).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * The inline {@code EXTERNAL} command is refused by the parser. It does not pass through the
     * {@code DatasetResolver} gate that closes {@code FROM <dataset>}, so without the parser guard it would reach
     * the operator-build backstop only after planning-time resolution had already tried to read {@code s3://}.
     *
     * <p>The wording follows {@link Federation#externalNotSupportedMessage()}: a node that could never run the
     * feature names the platform, one that merely has it switched off gives the generic message. Both are correct
     * here depending on the build, so the assertion asks rather than hardcoding.
     */
    public void testExternalCommandIsRefusedByTheParser() throws IOException {
        Request query = new Request("POST", "/_query");
        query.setJsonEntity("""
            {"query": "EXTERNAL \\"s3://bucket/data.parquet\\""}""");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(query));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString(Federation.externalNotSupportedMessage()));
    }

    /**
     * The dynamic-settings lever is shut too, so an operator cannot turn the feature on at runtime: an unregistered
     * feature registers no settings, making every federation key unknown. Mirrors
     * {@code FederationDisabledRestIT#testFederationSettingsAreRejectedOverRest}, except that here nothing was
     * configured to produce the state — the platform default alone did.
     */
    public void testFederationSettingsAreUnknown() throws IOException {
        Request update = new Request("PUT", "/_cluster/settings");
        update.setJsonEntity(Strings.format("""
            {"persistent": {"%s": true}}""", Federation.FEDERATION_ENABLED.getKey()));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(update));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("not recognized"));
    }
}
