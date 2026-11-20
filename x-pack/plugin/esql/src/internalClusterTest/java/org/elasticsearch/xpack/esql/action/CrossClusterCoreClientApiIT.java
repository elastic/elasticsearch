/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo.Status;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.ExecutionMetadata;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Pins the {@code _clusters} metadata and the time zone as seen by in-process clients of the x-pack core ES|QL API, across real
 * clusters. Every query here goes through {@link EsqlQueryRequestBuilder} from x-pack core, not the ES|QL plugin's own request class.
 */
public class CrossClusterCoreClientApiIT extends AbstractCrossClusterTestCase {

    private static final String CROSS_CLUSTER_PATTERN = "FROM logs-*," + REMOTE_CLUSTER_1 + ":logs-*";

    public void testLimitZeroReportsClustersWithCcsMetadata() throws Exception {
        setupClusters(2);
        try (var response = run(CROSS_CLUSTER_PATTERN + " | LIMIT 0", b -> b.executionMetadata(ExecutionMetadata.CCS_ONLY))) {
            assertBothClustersSuccessful(response.response().clusters());
            assertFalse(response.response().rows().iterator().hasNext());
        }
    }

    public void testDescribeReportsClustersWithCcsMetadata() throws Exception {
        assumeTrue("DESCRIBE is snapshot-only", EsqlCapabilities.Cap.DESCRIBE_COMMAND.isEnabled());
        setupClusters(2);
        try (var response = run(CROSS_CLUSTER_PATTERN + " | DESCRIBE", b -> b.executionMetadata(ExecutionMetadata.CCS_ONLY))) {
            assertBothClustersSuccessful(response.response().clusters());
        }
    }

    public void testLocalOnlyQueryWithExecutionMetadata() throws Exception {
        setupClusters(2);
        try (var response = run("FROM logs-* | LIMIT 0", b -> b.executionMetadata(ExecutionMetadata.ALWAYS))) {
            assertThat(
                response.response().clusters(),
                contains(new EsqlClusterInfo(EsqlClusterInfo.LOCAL_CLUSTER_ALIAS, "logs-*", Status.SUCCESSFUL))
            );
        }
    }

    public void testNoMetadataRequested() throws Exception {
        setupClusters(2);
        try (var response = run(CROSS_CLUSTER_PATTERN + " | LIMIT 0", b -> {})) {
            assertThat(response.response().clusters(), empty());
        }
    }

    public void testTimeZoneApplies() {
        String query = "ROW t = TO_DATETIME(\"2024-01-01T00:00:00Z\") | EVAL h = DATE_EXTRACT(\"hour_of_day\", t) | KEEP h";
        try (var response = run(query, b -> b.setting("time_zone", "Europe/Paris"))) {
            assertThat(singleValue(response), equalTo(1L));
        }
        try (var response = run(query, b -> {})) {
            assertThat(singleValue(response), equalTo(0L));
        }
    }

    private EsqlQueryResponse run(
        String query,
        Consumer<EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse>> configure
    ) {
        var builder = EsqlQueryRequestBuilder.newRequestBuilder(client(LOCAL_CLUSTER));
        builder.query(query);
        configure.accept(builder);
        return builder.execute().actionGet(30, TimeUnit.SECONDS);
    }

    private static Object singleValue(EsqlQueryResponse response) {
        var rows = response.response().rows().iterator();
        assertTrue(rows.hasNext());
        var values = rows.next().iterator();
        assertTrue(values.hasNext());
        Object value = values.next();
        assertFalse(values.hasNext());
        assertFalse(rows.hasNext());
        return value;
    }

    private static void assertBothClustersSuccessful(List<EsqlClusterInfo> clusters) {
        assertThat(
            clusters,
            contains(
                new EsqlClusterInfo(EsqlClusterInfo.LOCAL_CLUSTER_ALIAS, "logs-*", Status.SUCCESSFUL),
                new EsqlClusterInfo(REMOTE_CLUSTER_1, "logs-*", Status.SUCCESSFUL)
            )
        );
    }
}
