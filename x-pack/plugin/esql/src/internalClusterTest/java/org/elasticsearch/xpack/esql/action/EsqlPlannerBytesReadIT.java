/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end coverage for the {@code bytes_read} accounting added to {@link org.elasticsearch.compute.operator.DriverCompletionInfo}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlPlannerBytesReadIT extends AbstractEsqlIntegTestCase {

    public void testAggregateBytesReadIncludesPlannerAndOperatorContributions() {
        assumeTrue(
            "directory_metrics feature flag must be enabled to record store bytes",
            Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled()
        );

        String idx = "esql_planner_bytes";
        assertAcked(
            prepareCreate(idx).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("name", "type=keyword")
        );

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 50; i++) {
            bulk.add(prepareIndex(idx).setSource("name", "row_" + i));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        // Force-merge to a single segment so the term dictionary is consolidated.
        client().admin().indices().prepareForceMerge(idx).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(idx).get();

        // A WHERE clause guarantees at least the operator path reads from the term index, so the
        // aggregate is reliably non-zero.
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + idx + " | WHERE name LIKE \"row_2*\" | STATS c = COUNT(*)");
        request.profile(true);

        try (EsqlQueryResponse response = run(request)) {
            assertThat(response.profile(), notNullValue());
            long aggregate = response.bytesRead();
            long sumOperatorBytes = 0L;
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus op : driver.operators()) {
                    sumOperatorBytes += op.bytesRead();
                    if (op.status() instanceof LuceneSourceOperator.Status) {
                        assertThat(op.bytesRead(), greaterThan(0L));
                    }
                }
            }

            // The aggregate is the sum of operator-reported bytes plus any planner-time I/O the
            // SEARCH thread observed. The planner contribution can be 0 for queries whose
            // rewrites are fully lazy, so we only require the aggregate to be a non-strict
            // upper bound on the operator sum.
            assertThat("aggregate bytes_read must include all operator-reported bytes", aggregate, greaterThanOrEqualTo(sumOperatorBytes));
            // A WHERE clause hitting the term index must produce non-zero overall I/O.
            assertThat("wildcard predicate must produce some recorded I/O", aggregate, greaterThan(0L));
        }
    }
}
