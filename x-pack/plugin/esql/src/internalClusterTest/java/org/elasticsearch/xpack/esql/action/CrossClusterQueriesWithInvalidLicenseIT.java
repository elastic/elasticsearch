/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class CrossClusterQueriesWithInvalidLicenseIT extends AbstractEnrichBasedCrossClusterTestCase {

    private static final String LICENSE_ERROR_MESSAGE = "A valid Enterprise license is required to run ES|QL cross-cluster searches.";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithNonEnterpriseOrExpiredLicense.class);  // key plugin for the test
        return plugins;
    }

    public void testBasicCrossClusterQuery() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> runQuery("FROM *,*:* | LIMIT 5", requestIncludeMeta)
        );
        assertThat(e.getMessage(), containsString(LICENSE_ERROR_MESSAGE));
    }

    public void testMetadataCrossClusterQuery() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> runQuery("FROM events,*:* METADATA _index | SORT _index", requestIncludeMeta)
        );
        assertThat(e.getMessage(), containsString(LICENSE_ERROR_MESSAGE));
    }

    public void testQueryAgainstNonMatchingClusterWildcardPattern() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // since this wildcarded expression does not resolve to a valid remote cluster, it is not considered
        // a cross-cluster search and thus should not throw a license error
        String q = "FROM xremote*:events";
        {
            String limit1 = q + " | STATS count(*)";
            try (EsqlQueryResponse resp = runQuery(limit1, requestIncludeMeta)) {
                assertThat(resp.columns().size(), equalTo(1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            }

            String limit0 = q + " | LIMIT 0";
            try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                assertThat(resp.columns().size(), equalTo(1));
                assertThat(getValuesList(resp).size(), equalTo(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            }
        }
    }

    public void testCCSWithLimit0() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();

        // local only query does not need a valid Enterprise or Trial license
        try (EsqlQueryResponse resp = runQuery("FROM events | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(false));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        }

        // cross-cluster searches should fail with license error
        String q = randomFrom("FROM events,c1:* | LIMIT 0", "FROM c1:* | LIMIT 0");
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> runQuery(q, requestIncludeMeta));
        assertThat(e.getMessage(), containsString(LICENSE_ERROR_MESSAGE));
    }

    public void testSearchesWhereNonExistentClusterIsSpecified() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // this one query should be allowed since x* does not resolve to any known remote cluster
        try (EsqlQueryResponse resp = runQuery("FROM events,x*:no_such_index* | STATS count(*)", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));

            assertNotNull(executionInfo);
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER)));
            assertThat(executionInfo.isCrossClusterSearch(), is(false));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            // since this not a CCS, only the overall took time in the EsqlExecutionInfo matters
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        }

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> runQuery("FROM events,no_such_cluster:no_such_index* | STATS count(*)", requestIncludeMeta)
        );
        // with a valid license this would throw "no such remote cluster" exception, but without a valid license, should get a license error
        assertThat(e.getMessage(), containsString(LICENSE_ERROR_MESSAGE));
    }

    public void testEnrichWithHostsPolicy() {
        // local-only queries do not need an Enterprise or Trial license
        for (var mode : Enrich.Mode.values()) {
            String query = "FROM events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            try (EsqlQueryResponse resp = runQuery(query, null)) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(
                    rows,
                    equalTo(
                        List.of(
                            List.of(2L, "Android"),
                            List.of(1L, "Linux"),
                            List.of(1L, "MacOS"),
                            List.of(4L, "Windows"),
                            Arrays.asList(1L, (String) null)
                        )
                    )
                );
                assertFalse(resp.getExecutionInfo().isCrossClusterSearch());
            }
        }

        // cross-cluster query should fail due to not having valid Enterprise or Trial license
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();

        for (var mode : Enrich.Mode.values()) {
            String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> runQuery(query, requestIncludeMeta));
            assertThat(e.getMessage(), containsString("A valid Enterprise license is required to run ES|QL cross-cluster searches."));
        }

        for (var mode : Enrich.Mode.values()) {
            String query = "FROM *:events,events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> runQuery(query, requestIncludeMeta));
            assertThat(e.getMessage(), containsString("A valid Enterprise license is required to run ES|QL cross-cluster searches."));
        }
    }

    public void testAggThenEnrichRemote() {
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | stats c = COUNT(*) by os
            | %s
            | sort vendor
            """, enrichHosts(Enrich.Mode.ANY), enrichVendors(Enrich.Mode.REMOTE));
        var error = expectThrows(ElasticsearchStatusException.class, () -> runQuery(query, randomBoolean()).close());
        // with a valid license this would fail with "ENRICH with remote policy can't be executed after STATS", so ensure here
        // that the license error is detected first and returned rather than a VerificationException
        assertThat(error.getMessage(), containsString(LICENSE_ERROR_MESSAGE));
    }

    public void testEnrichCoordinatorThenEnrichRemote() {
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | %s
            | sort vendor
            """, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.REMOTE));
        var error = expectThrows(ElasticsearchStatusException.class, () -> runQuery(query, randomBoolean()).close());
        assertThat(
            error.getMessage(),
            // with a valid license the error is "ENRICH with remote policy can't be executed after another ENRICH with coordinator policy",
            // so ensure here that the license error is detected first and returned rather than a VerificationException
            containsString(LICENSE_ERROR_MESSAGE)
        );
    }
}
