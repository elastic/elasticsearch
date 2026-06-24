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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Covers the cross-project (CPS) detect-and-fail path of {@link DatasetResolver#replaceDatasets}: when CPS is enabled and a
 * FROM relation would resolve to a dataset in a linked project, the query must fail cleanly; otherwise the local rewrite
 * proceeds unchanged. The remote dispatch is exercised through a {@link DatasetRemoteResolver} test subclass that overrides
 * {@link DatasetRemoteResolver#anyLinkedProjectHasDataset} so no live transport is needed (documented at the subclass).
 */
public class DatasetResolverTests extends ESTestCase {

    private static final String DATASET_NAME = "logs";
    private static final String LINKED_PROJECT_ALIAS = "_linked";

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

    public void testCrossProjectFailsWhenLinkedProjectHasDataset() {
        AtomicInteger remoteCalls = new AtomicInteger();
        DatasetResolver resolver = resolver(crossProjectEnabled(true), remoteResolver(Set.of(LINKED_PROJECT_ALIAS), true, remoteCalls));

        VerificationException e = expectThrows(VerificationException.class, () -> replaceDatasets(resolver, relationOf(DATASET_NAME)));
        assertThat(e.getMessage(), containsString("FROM <dataset> is not yet supported with cross-project search"));
        assertEquals("remote leg must be consulted exactly once for the single relation", 1, remoteCalls.get());
    }

    public void testCrossProjectProceedsWhenNoLinkedProjectHasDataset() {
        AtomicInteger remoteCalls = new AtomicInteger();
        DatasetResolver resolver = resolver(crossProjectEnabled(true), remoteResolver(Set.of(LINKED_PROJECT_ALIAS), false, remoteCalls));

        LogicalPlan rewritten = replaceDatasets(resolver, relationOf(DATASET_NAME));
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        assertEquals(1, remoteCalls.get());
    }

    public void testCrossProjectWithNoLinkedProjectsSkipsRemoteLeg() {
        AtomicInteger remoteCalls = new AtomicInteger();
        // No linked projects registered (e.g. CPS on but nothing linked): the remote leg short-circuits and never runs.
        DatasetResolver resolver = resolver(crossProjectEnabled(true), remoteResolver(Set.of(), true, remoteCalls));

        LogicalPlan rewritten = replaceDatasets(resolver, relationOf(DATASET_NAME));
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        assertEquals(0, remoteCalls.get());
    }

    public void testNonCrossProjectSkipsRemoteLeg() {
        AtomicInteger remoteCalls = new AtomicInteger();
        DatasetResolver resolver = resolver(crossProjectEnabled(false), remoteResolver(Set.of(LINKED_PROJECT_ALIAS), true, remoteCalls));

        // CPS disabled: the remote leg is never consulted, so even a "would-detect" remote stub does not fail the query.
        LogicalPlan rewritten = replaceDatasets(resolver, relationOf(DATASET_NAME));
        assertThat(rewritten, instanceOf(UnresolvedExternalRelation.class));
        assertEquals("remote leg must not run when CPS is disabled", 0, remoteCalls.get());
    }

    // --- harness ---

    private LogicalPlan replaceDatasets(DatasetResolver resolver, UnresolvedRelation relation) {
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        resolver.replaceDatasets(relation, project(), future);
        return future.actionGet();
    }

    private DatasetResolver resolver(CrossProjectModeDecider decider, DatasetRemoteResolver remoteResolver) {
        return new DatasetResolver(localActionClient(), EsExecutors.DIRECT_EXECUTOR_SERVICE, decider, remoteResolver);
    }

    private static CrossProjectModeDecider crossProjectEnabled(boolean enabled) {
        return new CrossProjectModeDecider(Settings.builder().put("serverless.cross_project.enabled", enabled).build());
    }

    /**
     * A {@link DatasetRemoteResolver} that skips transport-handler registration (no live
     * {@link org.elasticsearch.transport.TransportService} in a unit test), reports {@code aliases} as the registered linked
     * projects, and answers the remote detection from {@code hasRemoteDataset} — recording how many times it ran.
     */
    private DatasetRemoteResolver remoteResolver(Set<String> aliases, boolean hasRemoteDataset, AtomicInteger calls) {
        return new DatasetRemoteResolver(null, null, null, null, false) {
            @Override
            public Set<String> linkedProjectAliases(ProjectId originProjectId) {
                return aliases;
            }

            @Override
            public void anyLinkedProjectHasDataset(
                String[] rawPatterns,
                Collection<String> linkedProjectAliases,
                ActionListener<Boolean> listener
            ) {
                calls.incrementAndGet();
                assertEquals(aliases, Set.copyOf(linkedProjectAliases));
                listener.onResponse(hasRemoteDataset);
            }
        };
    }

    /**
     * A client whose only registered behaviour is the local {@link EsqlResolveDatasetAction}: it reports the requested
     * dataset as authorized, so the no-detection path rewrites the relation into an {@link UnresolvedExternalRelation}.
     */
    private Client localActionClient() {
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
                listener.onResponse((Response) new EsqlResolveDatasetAction.Response(Set.of(DATASET_NAME), false, Set.of()));
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
