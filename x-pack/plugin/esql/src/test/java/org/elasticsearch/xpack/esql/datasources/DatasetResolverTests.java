/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.DatasetShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Covers {@link DatasetResolver#replaceDatasets}: the local read-authorization dispatch and rewrite of {@code FROM <dataset>}.
 * Cross-project remote-dataset detection no longer lives here — it rides the field-caps remote-detect rail (see
 * {@code EsqlResolveFieldsAction} + {@code RemoteDatasetNotSupportedException}); this resolver only performs the local rewrite
 * and, under CPS, preserves a wildcard sibling so the remote half reaches field-caps (exercised in {@code DatasetRewriterTests}).
 */
public class DatasetResolverTests extends ESTestCase {

    private static final String DATASET_NAME = "logs";

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testLocalDatasetRewrittenWhenCrossProjectEnabled() {
        AtomicInteger localCalls = new AtomicInteger();
        DatasetResolver resolver = resolver(crossProjectEnabled(true), localCalls);

        // Under CPS an exact dataset name keeps a DatasetShadowRelation sibling next to the local external relation so the
        // remote half of the exact name reaches field-caps (the exact-name analog of the wildcard-preserve path). The
        // shadow is later stripped by the analyzer when no linked index matches, returning to the bare external shape.
        LogicalPlan rewritten = replaceDatasets(resolver, relationOf(DATASET_NAME));
        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll unionAll = (UnionAll) rewritten;
        assertEquals(2, unionAll.children().size());
        assertTrue(
            "a local external relation must be present",
            unionAll.children().stream().anyMatch(c -> c instanceof UnresolvedExternalRelation)
        );
        assertTrue(
            "an exact-name dataset shadow must be emitted",
            unionAll.children().stream().anyMatch(c -> c instanceof DatasetShadowRelation)
        );
        assertEquals("local read-authz dispatch runs once for the single relation", 1, localCalls.get());
    }

    public void testLocalDatasetRewrittenWhenCrossProjectDisabled() {
        AtomicInteger localCalls = new AtomicInteger();
        DatasetResolver resolver = resolver(crossProjectEnabled(false), localCalls);

        LogicalPlan rewritten = replaceDatasets(resolver, relationOf(DATASET_NAME));
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        assertEquals(1, localCalls.get());
    }

    public void testWildcardMatchingDatasetUnderCpsPreservesRemoteHalf() {
        AtomicInteger localCalls = new AtomicInteger();
        DatasetResolver resolver = resolver(crossProjectEnabled(true), localCalls);

        // Under CPS a wildcard matching a local dataset keeps the original wildcard as a sibling UnresolvedRelation so the
        // remote half (and the field-caps remote-detect) is reached, alongside the local UnresolvedExternalRelation.
        LogicalPlan rewritten = replaceDatasets(resolver, relationOf("log*"));
        assertThat(rewritten, instanceOf(UnionAll.class));
        UnionAll unionAll = (UnionAll) rewritten;
        assertEquals(2, unionAll.children().size());
        assertTrue(
            "a local external relation must be present",
            unionAll.children().stream().anyMatch(c -> c instanceof UnresolvedExternalRelation)
        );
        assertTrue(
            "the wildcard must be preserved as a sibling UnresolvedRelation",
            unionAll.children().stream().anyMatch(c -> c instanceof UnresolvedRelation)
        );
    }

    public void testNoDatasetsRegisteredLeavesPlanUnchanged() {
        AtomicInteger localCalls = new AtomicInteger();
        DatasetResolver resolver = resolver(crossProjectEnabled(true), localCalls);

        UnresolvedRelation relation = relationOf(DATASET_NAME);
        LogicalPlan rewritten = replaceDatasets(resolver, relation, ProjectMetadata.builder(ProjectId.DEFAULT).build());
        assertSame(relation, rewritten);
        assertEquals("no datasets registered → no dispatch", 0, localCalls.get());
    }

    public void testFederationDisabledReturnsPlanUnchanged() {
        AtomicInteger localCalls = new AtomicInteger();
        DatasetResolver resolver = resolver(crossProjectEnabled(true), localCalls);

        // The kill switch suppresses all dataset resolution: the plan is returned untouched and no
        // EsqlResolveDatasetAction dispatch happens, even though datasets are registered in project state.
        UnresolvedRelation relation = relationOf(DATASET_NAME);
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.replaceDatasets(relation, project(), future, false);
        assertSame(relation, future.actionGet());
        assertEquals("federation disabled → no dispatch", 0, localCalls.get());
    }

    // --- harness ---

    private LogicalPlan replaceDatasets(DatasetResolver resolver, UnresolvedRelation relation) {
        return replaceDatasets(resolver, relation, project());
    }

    private LogicalPlan replaceDatasets(DatasetResolver resolver, UnresolvedRelation relation, ProjectMetadata project) {
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.replaceDatasets(relation, project, future);
        return future.actionGet();
    }

    private DatasetResolver resolver(CrossProjectModeDecider decider, AtomicInteger localCalls) {
        return new DatasetResolver(localActionClient(localCalls), EsExecutors.DIRECT_EXECUTOR_SERVICE, decider);
    }

    private static CrossProjectModeDecider crossProjectEnabled(boolean enabled) {
        return new CrossProjectModeDecider(Settings.builder().put("serverless.cross_project.enabled", enabled).build());
    }

    /**
     * A client whose only registered behaviour is the local {@link EsqlResolveDatasetAction}: it reports the requested
     * dataset as authorized, so the rewrite turns the relation into an {@link UnresolvedExternalRelation}. Counts the
     * dispatches so a test can assert the per-relation round-trip ran.
     */
    private Client localActionClient(AtomicInteger localCalls) {
        return new AbstractClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            @SuppressWarnings("unchecked")
            protected <
                Request extends org.elasticsearch.action.ActionRequest,
                Response extends org.elasticsearch.action.ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                assertSame(EsqlResolveDatasetAction.TYPE, action);
                localCalls.incrementAndGet();
                listener.onResponse((Response) new EsqlResolveDatasetAction.Response(Set.of(DATASET_NAME), Set.of(), Set.of()));
            }
        };
    }

    private static UnresolvedRelation relationOf(String pattern) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, pattern), false, List.of(), IndexMode.STANDARD, null);
    }

    private static ProjectMetadata project() {
        DataSource parent = new DataSource("s3_parent", "test", null, Map.<String, DataSourceSetting>of());
        Dataset dataset = new Dataset(DATASET_NAME, new DataSourceReference("s3_parent"), "s3://logs/*.parquet", null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("s3_parent", parent)))
            .datasets(Map.of(DATASET_NAME, dataset))
            .build();
    }
}
