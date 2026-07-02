/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.index.MappingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class EsqlSessionTests extends ESTestCase {

    public void testShouldRetryConcreteTimeSeriesResolution() {
        assertTrue(
            EsqlSession.shouldRetryConcreteTimeSeriesResolution(
                IndexMode.TIME_SERIES,
                IndexResolution.empty("logs"),
                new IndexPattern(EMPTY, "logs")
            )
        );
    }

    public void testShouldNotRetryWildcardTimeSeriesResolution() {
        assertFalse(
            EsqlSession.shouldRetryConcreteTimeSeriesResolution(
                IndexMode.TIME_SERIES,
                IndexResolution.empty("logs*"),
                new IndexPattern(EMPTY, "logs*")
            )
        );
    }

    public void testRefineConcreteTimeSeriesResolutionReturnsHelpfulError() {
        IndexResolution resolution = EsqlSession.refineConcreteTimeSeriesResolution(
            new IndexPattern(EMPTY, "logs"),
            IndexResolution.empty("logs"),
            resolvedIndex("logs")
        );

        MappingException e = expectThrows(MappingException.class, resolution::get);
        assertThat(e.getMessage(), containsString("[logs] is not a time series index. Use FROM command instead"));
    }

    public void testRefineConcreteTimeSeriesResolutionKeepsOriginalFailures() {
        FieldCapabilitiesFailure failure = new FieldCapabilitiesFailure(new String[] { "logs" }, new ElasticsearchException("boom"));
        IndexResolution originalResolution = IndexResolution.valid(
            new EsIndex("logs", Map.of(), Map.of(), Map.of(), Map.of()),
            Set.of(),
            Map.of("remote", List.of(failure))
        );

        IndexResolution resolution = EsqlSession.refineConcreteTimeSeriesResolution(
            new IndexPattern(EMPTY, "logs"),
            originalResolution,
            IndexResolution.empty("logs")
        );

        assertThat(resolution, sameInstance(originalResolution));
    }

    public void testExtractExternalConfigsThrowsOnNonLiteralTablePath() {
        // After parameter substitution at parse time, every UnresolvedExternalRelation tablePath is
        // expected to be a non-null Literal. extractExternalConfigs fails closed with
        // IllegalStateException rather than silently dropping the entry from the resulting map.
        Source source = Source.EMPTY;
        Expression nonLiteral = new UnresolvedAttribute(source, "?param");
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, nonLiteral, new HashMap<>());

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> EsqlSession.extractExternalConfigs(relation));
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

        Map<String, Map<String, Object>> result = EsqlSession.extractExternalConfigs(relation);
        assertThat(result, equalTo(Map.of("s3://bucket/table", config)));
    }

    public void testComputeLookupJoinIndexScope() {
        {
            // joining to on a local cluster
            var plan = TEST_PARSER.parseQuery("FROM index | LOOKUP JOIN lookup ON key | KEEP f1,f2,f3");
            var resolution = createIndexResolution("index");
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // joining on a remote cluster
            var plan = TEST_PARSER.parseQuery("FROM remote:index | LOOKUP JOIN lookup ON key | KEEP f1,f2,f3");
            var resolution = createIndexResolution("remote:index");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote")));
        }
        {
            // joining to a row
            var plan = TEST_PARSER.parseQuery("ROW key=1 | LOOKUP JOIN lookup ON key");
            var resolution = createIndexResolution();
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), empty());
        }
        {
            // multiple joins
            var plan = TEST_PARSER.parseQuery("""
                FROM index
                | LOOKUP JOIN lookup-1 ON key
                | LOOKUP JOIN lookup-2 ON key""");
            var resolution = createIndexResolution("index");
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup-1", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup-2", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // joining in subqueries
            var plan = TEST_PARSER.parseQuery("""
                FROM (FROM data | LOOKUP JOIN lookup-0 ON key),
                     (FROM remote-1:data | LOOKUP JOIN lookup-1 ON key),
                     (FROM remote-2:data | LOOKUP JOIN lookup-2 ON key)
                | LOOKUP JOIN lookup-3 ON key
                | KEEP key, cluster, location
                | SORT key
                """);
            var resolution = createIndexResolution("data", "remote-1:data", "remote-2:data");
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup-0", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup-1", resolution), equalTo(Set.of("remote-1")));
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup-2", resolution), equalTo(Set.of("remote-2")));
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup-3", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "remote-1", "remote-2"))
            );
        }
        {
            // joining same lookup from differently scoped subqueries
            var plan = TEST_PARSER.parseQuery("""
                FROM (FROM remote-1:data | LOOKUP JOIN lookup ON key),
                     (FROM remote-2:data | LOOKUP JOIN lookup ON key)
                | KEEP key, cluster, location
                | SORT key
                """);
            var resolution = createIndexResolution("remote-1:data", "remote-2:data");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote-1", "remote-2")));
        }
    }

    public void testComputeLookupJoinIndexScopeWhereInSubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());

        {
            // LOOKUP JOIN inside IN subquery on a local index
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("FROM main | WHERE x IN (FROM sub | LOOKUP JOIN lookup ON x)"));
            var resolution = createIndexResolution("main", "sub");
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // LOOKUP JOIN inside IN subquery on a remote index
            var plan = InSubqueryResolver.resolve(
                TEST_PARSER.parseQuery("FROM main | WHERE x IN (FROM remote:sub | LOOKUP JOIN lookup ON x)")
            );
            var resolution = createIndexResolution("main", "remote:sub");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote")));
        }
        {
            // LOOKUP JOIN at top level AND inside IN subquery — scope is the union of both sources
            var plan = InSubqueryResolver.resolve(
                TEST_PARSER.parseQuery("FROM main | LOOKUP JOIN lookup ON x | WHERE x IN (FROM remote:sub | LOOKUP JOIN lookup ON x)")
            );
            var resolution = createIndexResolution("main", "remote:sub");
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "remote"))
            );
        }
    }

    private static Map<IndexPattern, IndexResolution> createIndexResolution(String... indices) {
        return Arrays.stream(indices).collect(toMap(index -> new IndexPattern(EMPTY, index), index -> {
            var resolved = Map.of(RemoteClusterAware.splitIndexName(index).getClusterGroupingKey(), List.of(index));
            return IndexResolution.valid(new EsIndex(index, Map.of(), Map.of(), resolved, resolved));
        }));
    }

    /**
     * Wiring test: {@code preAnalyzeExternalSources} must forward the computed
     * {@code pathsRequiringStats} set — always non-null — to {@code ExternalSourceResolver#resolve}.
     * A {@code LIMIT}-shaped plan forwards an empty (defer-everything) set. Uses a capturing fake
     * resolver to assert the argument actually reaches {@code resolve(...)}.
     */
    public void testPreAnalyzeExternalSourcesForwardsEmptySetForLimit() {
        String path = "s3://bucket/data/*.parquet";
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(EMPTY, Literal.keyword(EMPTY, path), Map.of());
        LogicalPlan plan = new Limit(EMPTY, new Literal(EMPTY, 10, DataType.INTEGER), relation);

        Set<String> captured = capturePathsRequiringStats(plan, path);
        assertNotNull("wiring must forward a non-null set", captured);
        assertTrue("LIMIT forwards an empty set (defer everything)", captured.isEmpty());
    }

    /**
     * Wiring test: an ungrouped {@code STATS COUNT(*)} over an external relation forwards a set
     * containing the relation's path, so the resolver keeps eager all-file stats aggregation for it.
     */
    public void testPreAnalyzeExternalSourcesForwardsPathForUngroupedStats() {
        String path = "s3://bucket/data/*.parquet";
        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(EMPTY, Literal.keyword(EMPTY, path), Map.of());
        LogicalPlan plan = new Aggregate(EMPTY, relation, List.of(), List.of());

        assertEquals(Set.of(path), capturePathsRequiringStats(plan, path));
    }

    /**
     * Drives {@code EsqlSession#preAnalyzeExternalSources} with a capturing {@link ExternalSourceResolver}
     * and returns the {@code pathsRequiringStats} argument it forwarded to {@code resolve(...)}.
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

        PreAnalyzer.PreAnalysis preAnalysis = new PreAnalyzer.PreAnalysis(
            Map.of(),
            List.of(),
            List.of(),
            Set.of(),
            false,
            false,
            false,
            List.of(path),
            List.of()
        );
        EsqlSession.PreAnalysisResult result = new EsqlSession.PreAnalysisResult(Set.of(), Set.of());
        PlainActionFuture<EsqlSession.PreAnalysisResult> future = new PlainActionFuture<>();
        EsqlSession.preAnalyzeExternalSources(capturingResolver, plan, preAnalysis, result, future);
        future.actionGet();
        assertTrue("resolve must be invoked when icebergPaths is non-empty", resolveCalled.get());
        return captured.get();
    }

    private static IndexResolution resolvedIndex(String indexName) {
        return IndexResolution.valid(
            new EsIndex(indexName, Map.of(), Map.of(indexName, IndexMode.STANDARD), Map.of(), Map.of()),
            Set.of(indexName),
            Map.of()
        );
    }
}
