/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

/**
 * The verifier is the correctness boundary of the cache: default deny, so what matters is that the admitted set is
 * exactly the small allowlist and that everything else is refused with a reason.
 */
public class ShardResultCacheVerifierTests extends ESTestCase {

    public void testPlainAggregationIsCacheable() {
        assertCacheable("from test | stats x = avg(salary)");
        assertCacheable("from test | stats x = avg(salary) by languages");
        assertCacheable("from test | where emp_no > 10 | stats x = avg(salary) by languages");
        assertCacheable("from test | eval bonus = salary * 2 | stats x = sum(bonus) by languages");
        assertCacheable("from test | keep salary, languages | stats x = avg(salary) by languages");
    }

    public void testRowShapesAreRefused() {
        // Raw rows are what late materialization loads doc ids for, and a doc column cannot be serialized at all.
        assertRefused("from test | keep emp_no | limit 10", "fragment root is not an aggregation");
        assertRefused("from test | sort emp_no | limit 10", "fragment root is not an aggregation");
    }

    public void testAnUnlistedPlanNodeUnderTheAggregationIsRefused() {
        assertRefused("from test | mv_expand first_name | stats x = count(*) by first_name", "unsupported plan node");
        assertRefused("from test | dissect first_name \"%{a}\" | stats x = count(*) by a", "unsupported plan node");
        assertRefused("from test | sample 0.5 | stats x = count(*)", "unsupported plan node");
    }

    public void testProfiledRequestsAreRefused() {
        DataNodeRequest request = request("from test | stats x = avg(salary)");
        Configuration base = request.configuration();
        Configuration profiled = new Configuration(
            base.now(),
            base.locale(),
            base.username(),
            base.clusterName(),
            base.pragmas(),
            base.resultTruncationMaxSize(false),
            base.resultTruncationDefaultSize(false),
            base.query(),
            true,
            base.tables(),
            base.queryStartTimeNanos(),
            base.allowPartialResults(),
            base.resultTruncationMaxSize(true),
            base.resultTruncationDefaultSize(true),
            base.resolvedSettings(),
            base.viewQueries()
        );
        assertThat(ShardResultCacheVerifier.notCacheableReason(withConfiguration(request, profiled)), containsString("profiled"));
    }

    public void testRemoteFetchIsRefused() {
        DataNodeRequest request = request("from test | stats x = avg(salary)");
        DataNodeRequest retained = new DataNodeRequest(
            request.sessionId(),
            request.configuration(),
            request.clusterAlias(),
            request.shards(),
            request.aliasFilters(),
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction(),
            request.reductionLateMaterialization(),
            true
        );
        assertThat(ShardResultCacheVerifier.notCacheableReason(retained), containsString("remote fetch"));
    }

    public void testCrossClusterIsRefused() {
        DataNodeRequest request = request("from test | stats x = avg(salary)");
        DataNodeRequest remote = new DataNodeRequest(
            request.sessionId(),
            request.configuration(),
            "remote1",
            request.shards(),
            request.aliasFilters(),
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction(),
            request.reductionLateMaterialization(),
            request.retainSearchContexts()
        );
        assertThat(ShardResultCacheVerifier.notCacheableReason(remote), containsString("cross-cluster"));
    }

    private static void assertCacheable(String query) {
        assertThat(query, ShardResultCacheVerifier.notCacheableReason(request(query)), nullValue());
    }

    private static void assertRefused(String query, String reason) {
        assertThat(query, ShardResultCacheVerifier.notCacheableReason(request(query)), containsString(reason));
    }

    private static DataNodeRequest request(String query) {
        return ShardResultCacheKeyTests.request(query);
    }

    private static DataNodeRequest withConfiguration(DataNodeRequest request, Configuration configuration) {
        return new DataNodeRequest(
            request.sessionId(),
            configuration,
            request.clusterAlias(),
            request.shards(),
            request.aliasFilters(),
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction(),
            request.reductionLateMaterialization(),
            request.retainSearchContexts()
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
