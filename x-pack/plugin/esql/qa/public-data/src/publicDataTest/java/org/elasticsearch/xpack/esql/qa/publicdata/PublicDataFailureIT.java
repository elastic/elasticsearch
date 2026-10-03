/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.util.EntityUtils;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;
import org.junit.AfterClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Expected-failure cases: catalog variants carrying an {@code expect_failure:} block point
 * <em>deliberately wrong configurations</em> (mislabeled format/codec, mispointed globs,
 * nonexistent keys, zero-byte objects) at real pinned public objects. csv-spec cannot express
 * "this query must fail", so this sibling IT asserts a clean, attributable 4xx — never a 5xx,
 * never a hang, never silently-wrong rows.
 *
 * <p>Transient store trouble must be told apart from the expected failure before asserting: a
 * throttled bucket's 503 goes through the bounded-retry path (and, exhausted, fails the run as
 * {@code INFRA_FAIL}), while a genuine 5xx or an unexpected success where a 4xx was expected is
 * itself a defect, routed through the defect policy.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
public class PublicDataFailureIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = PublicDataClusters.shared();

    private final VariantSpec variant;

    public PublicDataFailureIT(@Name("variant") String label, VariantSpec variant) {
        this.variant = variant;
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> parameters() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        PublicDataFilters filters = PublicDataFilters.fromSystemProperties();
        List<Object[]> parameters = new ArrayList<>();
        catalog.corpora()
            .stream()
            .filter(filters::matches)
            .flatMap(corpus -> corpus.variants().stream())
            .filter(VariantSpec::active)
            .filter(variant -> variant.expectFailure() != null)
            .filter(filters::matches)
            .forEach(variant -> parameters.add(new Object[] { variant.label(), variant }));
        return parameters;
    }

    public void testFailsCleanly() throws Throwable {
        String dataset = DatasetRegistry.sanitizeDatasetName("pd_fail_", variant.label());
        PublicDataRetry.run("expect-failure " + variant.label(), () -> {
            ResponseException failure = registerAndQueryExpectingFailure(dataset);
            assertExpectedFailure(failure);
        });
    }

    /**
     * Registers the deliberately wrong configuration and queries it. The rejection may legitimately
     * surface at dataset registration or at query time — either is a clean refusal. Returns the
     * client error; throws (through the retry/INFRA_FAIL machinery) on transient trouble; fails the
     * test if everything succeeded, because reading garbage as rows is exactly the defect this
     * hunts.
     */
    private ResponseException registerAndQueryExpectingFailure(String dataset) throws IOException {
        try {
            String dataSource = DatasetRegistry.ensureDataSource(
                client(),
                variant.datasetSourceName(),
                variant.provider().esType(),
                variant.dataSourceSettings()
            );
            DatasetRegistry.putDataset(client(), dataset, dataSource, variant.resource(), variant.datasetSettings());
            Request query = new Request("POST", "/_query");
            query.setJsonEntity("{\"query\": \"FROM " + dataset + " | LIMIT 5\"}");
            client().performRequest(query);
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            if (status == 503 || status == 429) {
                throw e; // transient: bounded retries, then INFRA_FAIL
            }
            return e;
        } finally {
            // registration may have succeeded even when the query then failed; drop the
            // deliberately-broken dataset so it never leaks into another test on the shared cluster
            DatasetRegistry.deleteIgnoringMissing(client(), "/_query/dataset/" + dataset);
        }
        throw new AssertionError(
            "variant ["
                + variant.label()
                + "] was expected to fail ("
                + variant.expectFailure().reason()
                + ") but the query succeeded — a misconfiguration was silently read as rows"
        );
    }

    private void assertExpectedFailure(ResponseException failure) throws IOException {
        int status = failure.getResponse().getStatusLine().getStatusCode();
        String body = EntityUtils.toString(failure.getResponse().getEntity());
        assertTrue(
            "variant ["
                + variant.label()
                + "] must fail with a clean "
                + variant.expectFailure().status()
                + " ("
                + variant.expectFailure().reason()
                + "), got "
                + status
                + ": "
                + abbreviated(body),
            variant.expectFailure().statusMatches(status)
        );
        Pattern expected = Pattern.compile(variant.expectFailure().messageRegex(), Pattern.DOTALL);
        assertTrue(
            "variant ["
                + variant.label()
                + "] failure message does not match ["
                + variant.expectFailure().messageRegex()
                + "]: "
                + abbreviated(body),
            expected.matcher(body).find()
        );
    }

    private static String abbreviated(String body) {
        return body.length() <= 500 ? body : body.substring(0, 500) + "...";
    }

    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(adminClient());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "PublicDataFailureIT{%s}", variant.label());
    }
}
