/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DatasetSchemaProviderTests extends ESTestCase {

    public void testExtractExternalConfigsThrowsOnNonLiteralTablePath() {
        // After parameter substitution at parse time, every UnresolvedExternalRelation tablePath is
        // expected to be a non-null Literal. extractExternalConfigs fails closed with
        // IllegalStateException rather than silently dropping the entry from the resulting map.
        Source source = Source.EMPTY;
        Expression nonLiteral = new UnresolvedAttribute(source, "?param");
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, nonLiteral, new HashMap<>());

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> DatasetSchemaProvider.extractExternalConfigs(relation));
        assertThat(ex.getMessage(), containsString("UnresolvedExternalRelation tablePath is not a non-null Literal"));
    }

    public void testExtractExternalConfigsHandlesLiteralTablePath() {
        // Positive case: a Literal-tablePath relation produces a map keyed by the path string with
        // the relation's config as the value.
        Source source = Source.EMPTY;
        Expression tablePath = Literal.keyword(source, "s3://bucket/table");
        Map<String, Object> config = new HashMap<>();
        config.put("region", "us-east-1");
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, config);

        Map<String, Map<String, Object>> result = DatasetSchemaProvider.extractExternalConfigs(relation);
        assertThat(result, equalTo(Map.of("s3://bucket/table", config)));
    }

    /**
     * Wiring test: {@code resolveExternalSources} must forward the computed {@code pathsRequiringStats} set — always
     * non-null — to {@code ExternalSourceResolver#resolve} (the 5-arg overload). A {@code LIMIT}-shaped plan forwards an
     * empty (defer-everything) set. Uses a capturing fake resolver to assert the argument reaches {@code resolve(...)}.
     */
    public void testResolveExternalSourcesForwardsEmptySetForLimit() {
        String path = "s3://bucket/data/*.parquet";
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(EMPTY, Literal.keyword(EMPTY, path), Map.of());
        LogicalPlan plan = new Limit(EMPTY, new Literal(EMPTY, 10, DataType.INTEGER), relation);

        Set<String> captured = capturePathsRequiringStats(plan, path);
        assertNotNull("wiring must forward a non-null set", captured);
        assertTrue("LIMIT forwards an empty set (defer everything)", captured.isEmpty());
    }

    /**
     * Wiring test: an ungrouped {@code STATS COUNT(*)} over an external relation forwards a set containing the
     * relation's path, so the resolver keeps eager all-file stats aggregation for it.
     */
    public void testResolveExternalSourcesForwardsPathForUngroupedStats() {
        String path = "s3://bucket/data/*.parquet";
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(EMPTY, Literal.keyword(EMPTY, path), Map.of());
        LogicalPlan plan = new Aggregate(EMPTY, relation, List.of(), List.of());

        assertEquals(Set.of(path), capturePathsRequiringStats(plan, path));
    }

    /**
     * Drives {@code DatasetSchemaProvider#resolveExternalSources} with a capturing {@link ExternalSourceResolver} and
     * returns the {@code pathsRequiringStats} argument it forwarded to {@code resolve(...)}.
     */
    private static Set<String> capturePathsRequiringStats(LogicalPlan plan, String path) {
        AtomicReference<Set<String>> captured = new AtomicReference<>();
        AtomicBoolean resolveCalled = new AtomicBoolean();
        ExternalSourceResolver capturingResolver = new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, null) {
            @Override
            public void resolve(
                List<String> paths,
                Map<String, Map<String, Object>> pathConfigs,
                Map<String, List<PartitionFilterHintExtractor.PartitionFilterHint>> filterHints,
                Set<String> pathsRequiringStats,
                ActionListener<ExternalSourceResolution> listener
            ) {
                resolveCalled.set(true);
                captured.set(pathsRequiringStats);
                listener.onResponse(ExternalSourceResolution.EMPTY);
            }
        };
        DatasetSchemaProvider provider = new DatasetSchemaProvider(capturingResolver, null, EsExecutors.DIRECT_EXECUTOR_SERVICE, null);

        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        provider.resolveExternalSources(plan, List.of(path), future);
        future.actionGet();
        assertTrue("resolve must be invoked when icebergPaths is non-empty", resolveCalled.get());
        return captured.get();
    }
}
