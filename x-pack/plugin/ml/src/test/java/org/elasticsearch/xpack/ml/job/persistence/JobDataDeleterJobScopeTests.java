/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.retention.MockWritableIndexExpander;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.ml.job.persistence.JobScopedDeleteCheckTests.assertNoMissingJobScopeEvent;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Drives the real {@link JobDataDeleter} methods against a recording client and checks the job-scope invariant event. */
public class JobDataDeleterJobScopeTests extends ESTestCase {

    private static final String SHARED_INDEX = ".ml-anomalies-shared-000001";

    private final String jobId = "job-" + randomAlphaOfLength(8);
    private final List<DeleteByQueryRequest> deleteByQueryRequests = new ArrayList<>();
    private RecordingClient client;

    @Before
    public void setUpClient() {
        MockWritableIndexExpander.create(true);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = new RecordingClient(threadPool);
    }

    private final class RecordingClient extends NoOpClient {
        RecordingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            ActionResponse response;
            if (request instanceof DeleteByQueryRequest deleteByQueryRequest) {
                deleteByQueryRequests.add(deleteByQueryRequest);
                response = new BulkByPaginatedSearchResponse(
                    TimeValue.ZERO,
                    new BulkByPaginatedSearchTask.Status(Collections.emptyList(), null, 0f),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    false
                );
            } else if (request instanceof GetModelSnapshotsAction.Request) {
                ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setSnapshotId("snap-1").setSnapshotDocCount(2).build();
                response = new GetModelSnapshotsAction.Response(new QueryPage<>(List.of(snapshot), 1, ModelSnapshot.RESULTS_FIELD));
            } else if (request instanceof RefreshRequest) {
                response = new BroadcastResponse(0, 0, 0, List.of());
            } else if (request instanceof MultiSearchRequest multiSearchRequest) {
                var items = new MultiSearchResponse.Item[multiSearchRequest.requests().size()];
                for (int i = 0; i < items.length; i++) {
                    items[i] = new MultiSearchResponse.Item(SearchResponse.emptyResponseBuilder().build(), null);
                }
                MultiSearchResponse multiSearchResponse = new MultiSearchResponse(items, 0L);
                try {
                    listener.onResponse((Response) multiSearchResponse);
                } finally {
                    multiSearchResponse.decRef();
                }
                return;
            } else if (request instanceof GetAliasesRequest) {
                response = new GetAliasesResponse(Map.of(), Map.of());
            } else {
                throw new AssertionError("unexpected request [" + request.getClass().getSimpleName() + "]");
            }
            listener.onResponse((Response) response);
        }
    }

    public void testDeleteJobDocumentsShouldNotLogMissingJobScopeForAnyIssuedDeleteByQuery() {
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(resolver.concreteIndexNames(any(ClusterState.class), any(), any(String[].class))).thenReturn(new String[] { SHARED_INDEX });
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        doAnswer(invocation -> {
            ActionListener<Job.Builder> listener = invocation.getArgument(2);
            listener.onResponse(new Job.Builder(jobId));
            return null;
        }).when(jobConfigProvider).getJob(anyString(), any(), any());
        AtomicBoolean finished = new AtomicBoolean();

        assertNoMissingJobScopeEvent(
            () -> new JobDataDeleter(client, jobId, true).deleteJobDocuments(
                jobConfigProvider,
                resolver,
                ClusterState.EMPTY_STATE,
                acknowledged -> finished.set(true),
                e -> fail(e, "unexpected failure")
            )
        );

        assertTrue("deletion should run to completion", finished.get());
        // model snapshots + state, quantiles, categorizer state, annotations, results
        assertThat(deleteByQueryRequests, hasSize(greaterThanOrEqualTo(5)));
    }

    public void testDeleteModelSnapshotsShouldNotLogMissingJobScope() {
        ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setSnapshotId("snap-1").setSnapshotDocCount(2).build();

        assertNoMissingJobScopeEvent(
            () -> new JobDataDeleter(client, jobId).deleteModelSnapshots(
                List.of(snapshot),
                ActionTestUtils.assertNoFailureListener(response -> {})
            )
        );

        assertThat(deleteByQueryRequests, hasSize(1));
    }

    public void testDeleteAnnotationsAndResultsFromTimeShouldNotLogMissingJobScope() {
        assertNoMissingJobScopeEvent(() -> {
            JobDataDeleter deleter = new JobDataDeleter(client, jobId, randomBoolean());
            deleter.deleteAllAnnotations(ActionTestUtils.assertNoFailureListener(response -> {}));
            deleter.deleteAnnotations(1_000L, 2_000L, Set.of("event"), ActionTestUtils.assertNoFailureListener(response -> {}));
            deleter.deleteResultsFromTime(1_000L, ActionTestUtils.assertNoFailureListener(response -> {}));
        });

        assertThat(deleteByQueryRequests, hasSize(greaterThanOrEqualTo(3)));
    }

    public void testDeleteInterimResultsOnJobResultsAliasShouldNotLogMissingJobScope() {
        assertNoMissingJobScopeEvent(() -> new JobDataDeleter(client, jobId).deleteInterimResults());

        assertThat(deleteByQueryRequests, hasSize(1));
    }

    public void testBlankJobIdShouldLogMissingJobScopeViolation() {
        String blankJobId = " ".repeat(randomIntBetween(1, 1000));
        MockLog.assertThatLogger(
            () -> new JobDataDeleter(client, blankJobId).deleteResultsFromTime(1_000L, ActionTestUtils.assertNoFailureListener(r -> {})),
            JobScopedDeleteCheck.class,
            new MockLog.PatternSeenEventExpectation(
                "blank job id violation",
                JobScopedDeleteCheck.class.getCanonicalName(),
                Level.ERROR,
                Pattern.quote("[ml-invariant:delete_missing_job_id_filter]") + ".*"
            )
        );

        assertThat(deleteByQueryRequests, hasSize(1));
    }
}
