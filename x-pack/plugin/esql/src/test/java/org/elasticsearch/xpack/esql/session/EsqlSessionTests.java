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
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
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
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
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
            // joining to a row: a ROW has no index relation but produces data on the coordinator, so the lookup index must
            // be resolved on the local (coordinating) cluster
            var plan = TEST_PARSER.parseQuery("ROW key=1 | LOOKUP JOIN lookup ON key");
            var resolution = createIndexResolution();
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // main-query join over a union of a remote index and a ROW: both local and remote cluster are in scope
            var plan = TEST_PARSER.parseQuery("FROM remote:index, (ROW key=1) | LOOKUP JOIN lookup ON key");
            var resolution = createIndexResolution("remote:index");
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "remote"))
            );
        }
        {
            // join nested inside a ROW subquery: only the local cluster is in scope
            var plan = TEST_PARSER.parseQuery("FROM remote:index, (ROW key=1 | LOOKUP JOIN lookup ON key)");
            var resolution = createIndexResolution("remote:index");
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // join nested inside a subquery on remote index: only remote cluster is in scope
            var plan = TEST_PARSER.parseQuery("FROM (ROW key=1), (FROM remote:index | LOOKUP JOIN lookup ON key)");
            var resolution = createIndexResolution("remote:index");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote")));
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
        {
            // LOOKUP JOIN in the main query AFTER a WHERE whose IN subquery is a ROW — the ROW is a row filter that does not
            // feed the lookup, so the scope is the outer source only and the local cluster is NOT added
            var plan = InSubqueryResolver.resolve(
                TEST_PARSER.parseQuery("FROM remote:main | WHERE x IN (ROW x = 1) | LOOKUP JOIN lookup ON x")
            );
            var resolution = createIndexResolution("remote:main");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote")));
        }
        {
            // LOOKUP JOIN in the main query AFTER a WHERE whose IN subquery reads from another remote — that remote is a row
            // filter source, not a lookup source, so the scope is the outer source only
            var plan = InSubqueryResolver.resolve(
                TEST_PARSER.parseQuery("FROM remote:main | WHERE x IN (FROM other:sub) | LOOKUP JOIN lookup ON x")
            );
            var resolution = createIndexResolution("remote:main", "other:sub");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote")));
        }
    }

    /**
     * Exercises {@link EsqlSession#computeLookupJoinIndexScope} on mixed shapes that combine FROM subqueries, WHERE IN
     * subqueries and nested IN subqueries, each referencing different local/remote clusters. The key invariant is that a
     * lookup is scoped to the clusters that actually feed rows into it (the data-bearing left spine), never to the source of
     * a sibling FROM-union branch nor to an IN subquery used only as a row filter.
     */
    public void testComputeLookupJoinIndexScopeMixedSubqueries() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());

        {
            // FROM subquery has a WHERE IN subquery, the LOOKUP JOIN sits AFTER that WHERE inside the same FROM subquery.
            // The lookup reads from remote-1 (the FROM subquery's own source); neither the IN-filter source remote-2 nor the
            // sibling FROM-union branch remote-3 feed it.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM (FROM remote-1:a | WHERE x IN (FROM remote-2:b) | LOOKUP JOIN lookup ON x),
                     (FROM remote-3:c)
                """));
            var resolution = createIndexResolution("remote-1:a", "remote-2:b", "remote-3:c");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote-1")));
        }
        {
            // FROM subquery has a WHERE IN subquery, the LOOKUP JOIN sits INSIDE that IN subquery. The lookup reads from
            // remote-2 (the IN subquery's source) only.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM (FROM remote-1:a | WHERE x IN (FROM remote-2:b | LOOKUP JOIN lookup ON x)),
                     (FROM remote-3:c)
                """));
            var resolution = createIndexResolution("remote-1:a", "remote-2:b", "remote-3:c");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote-2")));
        }
        {
            // The WHERE IN subquery is itself a union of two FROM subqueries, each carrying its own LOOKUP JOIN. The scope is
            // the union of the two subquery sources; the outer local index `main` is only a filtered source, not a lookup one.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM main
                | WHERE x IN (FROM (FROM remote-1:a | LOOKUP JOIN lookup ON x),
                                   (FROM remote-2:b | LOOKUP JOIN lookup ON x))
                """));
            var resolution = createIndexResolution("main", "remote-1:a", "remote-2:b");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote-1", "remote-2")));
        }
        {
            // The WHERE IN subquery is a union of two FROM subqueries, but the LOOKUP JOIN sits in the main query AFTER the
            // WHERE. The lookup reads only from the outer source remote-0; the IN-filter sources remote-1/remote-2 are excluded.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM remote-0:main
                | WHERE x IN (FROM (FROM remote-1:a), (FROM remote-2:b))
                | LOOKUP JOIN lookup ON x
                """));
            var resolution = createIndexResolution("remote-0:main", "remote-1:a", "remote-2:b");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote-0")));
        }
        {
            // Nested IN subqueries (outer IN -> inner IN), the LOOKUP JOIN sits INSIDE the innermost subquery. The lookup reads
            // from remote-2 only; the intermediate remote-1 and the outermost remote-0 are filter sources.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM remote-0:main
                | WHERE x IN (FROM remote-1:a | WHERE y IN (FROM remote-2:b | LOOKUP JOIN lookup ON y))
                """));
            var resolution = createIndexResolution("remote-0:main", "remote-1:a", "remote-2:b");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote-2")));
        }
        {
            // Nested IN subqueries, the LOOKUP JOIN sits AFTER the inner WHERE but inside the outer IN subquery. The lookup
            // reads from remote-1 (the outer IN subquery's own source); the inner IN-filter source remote-2 and the outermost
            // remote-0 are excluded.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM remote-0:main
                | WHERE x IN (FROM remote-1:a | WHERE y IN (FROM remote-2:b) | LOOKUP JOIN lookup ON y)
                """));
            var resolution = createIndexResolution("remote-0:main", "remote-1:a", "remote-2:b");
            assertThat(EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution), equalTo(Set.of("remote-1")));
        }
        {
            // Everything at once: a FROM-union whose first branch carries a LOOKUP JOIN (local), a WHERE IN subquery carrying
            // another LOOKUP JOIN (remote-2), and a top-level LOOKUP JOIN after the WHERE. The top-level lookup reads from the
            // whole FROM-union (local + remote-1). The scope is the union of all three lookups' data sources.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM (FROM local-main | LOOKUP JOIN lookup ON x),
                     (FROM remote-1:b)
                | WHERE x IN (FROM remote-2:c | LOOKUP JOIN lookup ON x)
                | LOOKUP JOIN lookup ON x
                """));
            var resolution = createIndexResolution("local-main", "remote-1:b", "remote-2:c");
            assertThat(
                EsqlSession.computeLookupJoinIndexScope(plan, "lookup", resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "remote-1", "remote-2"))
            );
        }
    }

    public void testComputeEnrichScope() {
        {
            // enrich on a local index
            var plan = TEST_PARSER.parseQuery("FROM index | ENRICH policy ON key");
            var resolution = createIndexResolution("index");
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // enrich on a remote index
            var plan = TEST_PARSER.parseQuery("FROM remote:index | ENRICH policy ON key");
            var resolution = createIndexResolution("remote:index");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote")));
        }
        {
            // enrich on a row: a ROW has no index relation but produces data on the coordinator, so the policy must be
            // resolved on the local (coordinating) cluster
            var plan = TEST_PARSER.parseQuery("ROW key=1 | ENRICH policy ON key");
            var resolution = createIndexResolution();
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // main-query enrich over a union of a remote index and a ROW: both local and remote cluster are in scope
            var plan = TEST_PARSER.parseQuery("FROM remote:index, (ROW key=1) | ENRICH policy ON key");
            var resolution = createIndexResolution("remote:index");
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "remote"))
            );
        }
        {
            // enrich nested inside a ROW subquery branch: only the local cluster is in scope
            var plan = TEST_PARSER.parseQuery("FROM remote:index, (ROW key=1 | ENRICH policy ON key)");
            var resolution = createIndexResolution("remote:index");
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // enrich nested inside a subquery branch on a remote index: only the remote cluster is in scope
            var plan = TEST_PARSER.parseQuery("FROM (ROW key=1), (FROM remote:index | ENRICH policy ON key)");
            var resolution = createIndexResolution("remote:index");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote")));
        }
        {
            // multiple enriches over the same local source
            var plan = TEST_PARSER.parseQuery("""
                FROM index
                | ENRICH policy-1 ON key
                | ENRICH policy-2 ON key""");
            var resolution = createIndexResolution("index");
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy-1"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy-2"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // enriches in FROM subqueries, plus one in the main query after the union
            var plan = TEST_PARSER.parseQuery("""
                FROM (FROM data | ENRICH policy-0 ON key),
                     (FROM remote-1:data | ENRICH policy-1 ON key),
                     (FROM remote-2:data | ENRICH policy-2 ON key)
                | ENRICH policy-3 ON key
                | KEEP key, cluster, location
                | SORT key
                """);
            var resolution = createIndexResolution("data", "remote-1:data", "remote-2:data");
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy-0"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy-1"), resolution), equalTo(Set.of("remote-1")));
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy-2"), resolution), equalTo(Set.of("remote-2")));
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy-3"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "remote-1", "remote-2"))
            );
        }
    }

    public void testComputeEnrichScopeWhereInSubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());

        {
            // ENRICH inside IN subquery on a local index
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("FROM main | WHERE x IN (FROM sub | ENRICH policy ON x)"));
            var resolution = createIndexResolution("main", "sub");
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
        }
        {
            // ENRICH inside IN subquery on a remote index
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("FROM main | WHERE x IN (FROM remote:sub | ENRICH policy ON x)"));
            var resolution = createIndexResolution("main", "remote:sub");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote")));
        }
        {
            // ENRICH at top level AND inside the IN subquery: each is scoped independently to its own feeding source
            var plan = InSubqueryResolver.resolve(
                TEST_PARSER.parseQuery("FROM main | ENRICH policy-a ON x | WHERE x IN (FROM remote:sub | ENRICH policy-b ON x)")
            );
            var resolution = createIndexResolution("main", "remote:sub");
            assertThat(
                EsqlSession.computeEnrichScope(enrichNamed(plan, "policy-a"), resolution),
                equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            );
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy-b"), resolution), equalTo(Set.of("remote")));
        }
        {
            // ENRICH in the main query AFTER a WHERE whose IN subquery is a ROW - the ROW is a row filter that does not feed
            // the enrich, so the scope is the outer source only and the local cluster is NOT added
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("FROM remote:main | WHERE x IN (ROW x = 1) | ENRICH policy ON x"));
            var resolution = createIndexResolution("remote:main");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote")));
        }
        {
            // ENRICH in the main query AFTER a WHERE whose IN subquery reads from another remote - that remote is a row
            // filter source, not an enrich source, so the scope is the outer source only
            var plan = InSubqueryResolver.resolve(
                TEST_PARSER.parseQuery("FROM remote:main | WHERE x IN (FROM other:sub) | ENRICH policy ON x")
            );
            var resolution = createIndexResolution("remote:main", "other:sub");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote")));
        }
        {
            // same policy name in both the outer query and inside the IN subquery: each occurrence at its own source position
            // gets an independent scope — outer sees the local source, the IN subquery's occurrence sees the remote source.
            var plan = InSubqueryResolver.resolve(
                TEST_PARSER.parseQuery("FROM main | ENRICH policy-all ON x | WHERE x IN (FROM remote:sub | ENRICH policy-all ON x)")
            );
            var resolution = createIndexResolution("main", "remote:sub");
            List<Enrich> allPolicyAlls = enrichesNamed(plan, "policy-all");
            assertThat(allPolicyAlls, hasSize(2));
            assertThat(
                allPolicyAlls.stream().map(e -> EsqlSession.computeEnrichScope(e, resolution)).collect(toSet()),
                equalTo(Set.of(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY), Set.of("remote")))
            );
        }
    }

    /**
     * Exercises {@link EsqlSession#computeEnrichScope} on mixed shapes that combine FROM subqueries and WHERE IN subqueries,
     * mirroring {@link #testComputeLookupJoinIndexScopeMixedSubqueries}. The key invariant is that an ENRICH is scoped to the
     * clusters that actually feed rows into it, never to the source of a sibling FROM-union branch nor to an IN subquery used
     * only as a row filter.
     */
    public void testComputeEnrichScopeMixedSubqueries() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());

        {
            // FROM subquery has a WHERE IN subquery, the ENRICH sits AFTER that WHERE inside the same FROM subquery. It reads
            // from remote-1 (the FROM subquery's own source); neither the IN-filter source remote-2 nor the sibling FROM-union
            // branch remote-3 feed it.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM (FROM remote-1:a | WHERE x IN (FROM remote-2:b) | ENRICH policy ON x),
                     (FROM remote-3:c)
                """));
            var resolution = createIndexResolution("remote-1:a", "remote-2:b", "remote-3:c");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote-1")));
        }
        {
            // FROM subquery has a WHERE IN subquery, the ENRICH sits INSIDE that IN subquery. It reads from remote-2 (the IN
            // subquery's source) only.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM (FROM remote-1:a | WHERE x IN (FROM remote-2:b | ENRICH policy ON x)),
                     (FROM remote-3:c)
                """));
            var resolution = createIndexResolution("remote-1:a", "remote-2:b", "remote-3:c");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote-2")));
        }
        {
            // The WHERE IN subquery is a union of two FROM subqueries, but the ENRICH sits in the main query AFTER the WHERE.
            // It reads only from the outer source remote-0; the IN-filter sources remote-1/remote-2 are excluded.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM remote-0:main
                | WHERE x IN (FROM (FROM remote-1:a), (FROM remote-2:b))
                | ENRICH policy ON x
                """));
            var resolution = createIndexResolution("remote-0:main", "remote-1:a", "remote-2:b");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote-0")));
        }
        {
            // Nested IN subqueries (outer IN -> inner IN), the ENRICH sits INSIDE the innermost subquery. It reads from
            // remote-2 only; the intermediate remote-1 and the outermost remote-0 are filter sources.
            var plan = InSubqueryResolver.resolve(TEST_PARSER.parseQuery("""
                FROM remote-0:main
                | WHERE x IN (FROM remote-1:a | WHERE y IN (FROM remote-2:b | ENRICH policy ON y))
                """));
            var resolution = createIndexResolution("remote-0:main", "remote-1:a", "remote-2:b");
            assertThat(EsqlSession.computeEnrichScope(enrichNamed(plan, "policy"), resolution), equalTo(Set.of("remote-2")));
        }
    }

    /**
     * Two distinct {@link Enrich} occurrences can share the exact same {@link Source} today: a view containing an ENRICH is
     * re-parsed independently every time it's referenced, so referencing it from two differently-scoped subquery branches
     * produces two structurally-identical (and therefore {@code Source.equals()}) {@link Enrich} nodes. Parsing the same
     * literal query text twice below reproduces that same-Source collision without needing an actual view.
     * <p>
     * {@link EsqlSession#computeEnrichScopes} unions the two occurrences' scopes rather than letting the second overwrite the
     * first. This also exercises the case where {@link EsqlCCSUtils#onlyRunning} hands back an immutable {@code Set.of(...)}
     * (no cluster tracked yet) for the first occurrence - that value must not be mutated in place once the second,
     * same-Source occurrence is merged into it.
     */
    public void testComputeEnrichScopesUnionsDuplicateSource() {
        var plan1 = TEST_PARSER.parseQuery("ROW key = 1 | ENRICH policy ON key");
        var plan2 = TEST_PARSER.parseQuery("ROW key = 1 | ENRICH policy ON key");
        Enrich enrich1 = enrichNamed(plan1, "policy");
        Enrich enrich2 = enrichNamed(plan2, "policy");
        assertThat(enrich1, not(sameInstance(enrich2)));
        assertThat(enrich1.source(), equalTo(enrich2.source()));

        var resolution = createIndexResolution();
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);

        Map<Source, Set<String>> scopes = EsqlSession.computeEnrichScopes(List.of(enrich1, enrich2), resolution, executionInfo);

        assertThat(scopes.keySet(), hasSize(1));
        assertThat(scopes.get(enrich1.source()), equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)));
    }

    /**
     * Returns the sole {@link Enrich} node with the given (resolved) policy name in {@code plan}, failing if there isn't
     * exactly one - test queries give each ENRICH occurrence under test a distinct policy name to disambiguate it.
     */
    private static Enrich enrichNamed(LogicalPlan plan, String policyName) {
        List<Enrich> matches = enrichesNamed(plan, policyName);
        assertThat(matches, hasSize(1));
        return matches.get(0);
    }

    /**
     * Returns all {@link Enrich} nodes with the given (resolved) policy name in {@code plan}, in post-order traversal order.
     * Use this when the same policy name intentionally appears multiple times (e.g. the same-policy-different-branch tests).
     */
    private static List<Enrich> enrichesNamed(LogicalPlan plan, String policyName) {
        List<Enrich> matches = new ArrayList<>();
        plan.forEachUp(Enrich.class, e -> {
            if (e.resolvedPolicyName().equals(policyName)) {
                matches.add(e);
            }
        });
        return matches;
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
                Map<String, org.elasticsearch.cluster.metadata.DatasetMapping> declaredMappings,
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

    public void testSuppliedSettingNamesCountsBothSurfaces() {
        // A setting supplied via the request body and a different one via in-query SET are both counted.
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(null);
        request.set(QuerySettings.TIME_ZONE, ZoneOffset.UTC);
        QuerySetting projectRouting = new QuerySetting(EMPTY, new Alias(EMPTY, "project_routing", Literal.keyword(EMPTY, "p")));
        EsqlStatement statement = new EsqlStatement(null, List.of(projectRouting));
        assertThat(EsqlSession.suppliedSettingNames(request, statement), equalTo(Set.of("time_zone", "project_routing")));
    }
}
