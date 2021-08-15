/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction.Request;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests.addJobTask;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCloseJobActionTests extends ESTestCase {

    private ClusterService clusterService;
    private Client client;
    private JobConfigProvider jobConfigProvider;
    private DatafeedConfigProvider datafeedConfigProvider;

    @Before
    public void setupMocks() {
        clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        jobConfigProvider = mock(JobConfigProvider.class);
        datafeedConfigProvider = mock(DatafeedConfigProvider.class);
    }

    public void testAddJobAccordingToState() {
        List<String> openJobIds = new ArrayList<>();
        List<String> closingJobIds = new ArrayList<>();
        List<String> failedJobIds = new ArrayList<>();

        PersistentTasksCustomMetadata.Builder taskBuilder =  PersistentTasksCustomMetadata.builder();
        addJobTask("open-job", null, JobState.OPENED, taskBuilder);
        addJobTask("failed-job", null, JobState.FAILED, taskBuilder);
        addJobTask("closing-job", null, JobState.CLOSING, taskBuilder);
        addJobTask("opening-job", null, JobState.OPENING, taskBuilder);
        PersistentTasksCustomMetadata tasks = taskBuilder.build();

        for (String id : new String [] {"open-job", "closing-job", "opening-job", "failed-job"}) {
            TransportCloseJobAction.addJobAccordingToState(id, tasks, openJobIds, closingJobIds, failedJobIds);
        }
        assertThat(openJobIds, containsInAnyOrder("open-job", "opening-job"));
        assertThat(failedJobIds, contains("failed-job"));
        assertThat(closingJobIds, contains("closing-job"));
    }

    @SuppressWarnings("unchecked")
    public void testStopDatafeedsIfNecessary() {
        final PersistentTasksCustomMetadata.Builder datafeedStartedTaskBuilder = PersistentTasksCustomMetadata.builder();
        String jobId = "job-with-started-df";
        String datafeedId = "df1";
        addJobTask(jobId, null, JobState.OPENED, datafeedStartedTaskBuilder);
        addTask(datafeedId, 0L, null, DatafeedState.STARTED, datafeedStartedTaskBuilder);

        mockDatafeedConfigFindDatafeeds(Collections.singleton(datafeedId));
        mockClientStopDatafeed();

        TransportCloseJobAction closeJobAction = createAction();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicBoolean responseHolder = new AtomicBoolean();
        ActionListener<Boolean> listener = ActionListener.wrap(responseHolder::set, exceptionHolder::set);

        TransportCloseJobAction.OpenAndClosingIds openAndClosingIds = new TransportCloseJobAction.OpenAndClosingIds();
        openAndClosingIds.openJobIds = Collections.singletonList(jobId);

        closeJobAction.stopDatafeedsIfNecessary(openAndClosingIds, false, TimeValue.ZERO, datafeedStartedTaskBuilder.build(), listener);

        assertTrue(responseHolder.get());
        assertNull(exceptionHolder.get());

        final PersistentTasksCustomMetadata.Builder datafeedNotStartedTaskBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(jobId, null, JobState.OPENED, datafeedNotStartedTaskBuilder);
        if (randomBoolean()) {
            addTask(datafeedId, 0L, null, DatafeedState.STOPPED, datafeedNotStartedTaskBuilder);
        }

        exceptionHolder.set(null);
        closeJobAction.stopDatafeedsIfNecessary(openAndClosingIds, false, TimeValue.ZERO, datafeedNotStartedTaskBuilder.build(), listener);
        assertFalse(responseHolder.get());
        assertNull(exceptionHolder.get());
        verify(client).execute(same(StopDatafeedAction.INSTANCE), any(StopDatafeedAction.Request.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    public void testStopDatafeedsIfNecessaryWithForce() {
        final PersistentTasksCustomMetadata.Builder datafeedStartedTaskBuilder = PersistentTasksCustomMetadata.builder();
        String jobId = "job-with-started-df";
        String datafeedId = "df1";
        addJobTask(jobId, null, JobState.OPENED, datafeedStartedTaskBuilder);
        addTask(datafeedId, 0L, null, DatafeedState.STARTED, datafeedStartedTaskBuilder);

        mockDatafeedConfigFindDatafeeds(Collections.singleton(datafeedId));
        mockClientStopDatafeed();
        mockClientIsolateDatafeed();

        TransportCloseJobAction closeJobAction = createAction();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicBoolean responseHolder = new AtomicBoolean();
        ActionListener<Boolean> listener = ActionListener.wrap(responseHolder::set, exceptionHolder::set);

        TransportCloseJobAction.OpenAndClosingIds openAndClosingIds = new TransportCloseJobAction.OpenAndClosingIds();
        openAndClosingIds.openJobIds = Collections.singletonList(jobId);

        closeJobAction.stopDatafeedsIfNecessary(openAndClosingIds, true, TimeValue.ZERO, datafeedStartedTaskBuilder.build(), listener);

        assertTrue(responseHolder.get());
        assertNull(exceptionHolder.get());

        final PersistentTasksCustomMetadata.Builder datafeedNotStartedTaskBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(jobId, null, JobState.OPENED, datafeedNotStartedTaskBuilder);
        if (randomBoolean()) {
            addTask(datafeedId, 0L, null, DatafeedState.STOPPED, datafeedNotStartedTaskBuilder);
        }

        exceptionHolder.set(null);
        closeJobAction.stopDatafeedsIfNecessary(openAndClosingIds, false, TimeValue.ZERO, datafeedNotStartedTaskBuilder.build(), listener);
        assertFalse(responseHolder.get());
        assertNull(exceptionHolder.get());
        verify(client).execute(same(IsolateDatafeedAction.INSTANCE), any(IsolateDatafeedAction.Request.class), any(ActionListener.class));
        verify(client).execute(same(StopDatafeedAction.INSTANCE), any(StopDatafeedAction.Request.class), any(ActionListener.class));
    }

    public void testValidate_givenFailedJob() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id_failed", null, JobState.FAILED, tasksBuilder);

        mockDatafeedConfigFindDatafeeds(Collections.emptySet());

        TransportCloseJobAction closeJobAction = createAction();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<TransportCloseJobAction.OpenAndClosingIds> responseHolder = new AtomicReference<>();
        ActionListener<TransportCloseJobAction.OpenAndClosingIds> listener = ActionListener.wrap(
                responseHolder::set,
                exceptionHolder::set
        );

        // force close so not an error for the failed job
        closeJobAction.validate(Collections.singletonList("job_id_failed"), true, tasksBuilder.build(), listener);
        assertNull(exceptionHolder.get());
        assertNotNull(responseHolder.get());
        assertThat(responseHolder.get().openJobIds, contains("job_id_failed"));
        assertThat(responseHolder.get().closingJobIds, empty());

        // not a force close so is an error
        responseHolder.set(null);
        closeJobAction.validate(Collections.singletonList("job_id_failed"), false, tasksBuilder.build(), listener);
        assertNull(responseHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException esException = (ElasticsearchStatusException) exceptionHolder.get();
        assertEquals(RestStatus.CONFLICT, esException.status());
        assertEquals("cannot close job [job_id_failed] because it failed, use force close", esException.getMessage());
    }

    public void testValidate_withSpecificJobIds() {
        PersistentTasksCustomMetadata.Builder tasksBuilder =  PersistentTasksCustomMetadata.builder();
        addJobTask("job_id_closing", null, JobState.CLOSING, tasksBuilder);
        addJobTask("job_id_open-1", null, JobState.OPENED, tasksBuilder);
        addJobTask("job_id_open-2", null, JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        mockDatafeedConfigFindDatafeeds(Collections.emptySet());

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<TransportCloseJobAction.OpenAndClosingIds> responseHolder = new AtomicReference<>();
        ActionListener<TransportCloseJobAction.OpenAndClosingIds> listener = ActionListener.wrap(
                responseHolder::set,
                exceptionHolder::set
        );

        TransportCloseJobAction closeJobAction = createAction();
        closeJobAction.validate(Arrays.asList("job_id_closing", "job_id_open-1", "job_id_open-2"), false, tasks, listener);
        assertNull(exceptionHolder.get());
        assertNotNull(responseHolder.get());
        assertEquals(Arrays.asList("job_id_open-1", "job_id_open-2"), responseHolder.get().openJobIds);
        assertEquals(Collections.singletonList("job_id_closing"), responseHolder.get().closingJobIds);

        closeJobAction.validate(Arrays.asList("job_id_open-1", "job_id_open-2"), false, tasks, listener);
        assertNull(exceptionHolder.get());
        assertNotNull(responseHolder.get());
        assertEquals(Arrays.asList("job_id_open-1", "job_id_open-2"), responseHolder.get().openJobIds);
        assertEquals(Collections.emptyList(), responseHolder.get().closingJobIds);

        closeJobAction.validate(Collections.singletonList("job_id_closing"), false, tasks, listener);
        assertNull(exceptionHolder.get());
        assertNotNull(responseHolder.get());
        assertEquals(Collections.emptyList(), responseHolder.get().openJobIds);
        assertEquals(Collections.singletonList("job_id_closing"), responseHolder.get().closingJobIds);

        closeJobAction.validate(Collections.singletonList("job_id_open-1"), false, tasks, listener);
        assertNull(exceptionHolder.get());
        assertNotNull(responseHolder.get());
        assertEquals(Collections.singletonList("job_id_open-1"), responseHolder.get().openJobIds);
        assertEquals(Collections.emptyList(), responseHolder.get().closingJobIds);
    }

    public void testDoExecute_whenNothingToClose() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(BaseMlIntegTestCase.createFareQuoteJob("foo").build(new Date()), false);

        PersistentTasksCustomMetadata.Builder tasksBuilder =  PersistentTasksCustomMetadata.builder();
        addJobTask("foo", null, JobState.CLOSED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metadata(new Metadata.Builder().putCustom(PersistentTasksCustomMetadata.TYPE,  tasksBuilder.build()))
                .build();

        TransportCloseJobAction transportAction = createAction();
        when(clusterService.state()).thenReturn(clusterState);
        SortedSet<String> expandedIds = new TreeSet<>();
        expandedIds.add("foo");
        mockJobConfigProviderExpandIds(expandedIds);
        mockDatafeedConfigFindDatafeeds(Collections.emptySortedSet());

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        CloseJobAction.Request request = new Request("foo");
        request.setLocal(true); // hack but it saves a lot of mocking
        // This method should return immediately because the job is already closed.
        // Check that the listener is called. If a different code path was taken the
        // listener wouldn't be called without extensive mocking
        transportAction.doExecute(mock(Task.class), request, new ActionListener<CloseJobAction.Response>() {
            @Override
            public void onResponse(CloseJobAction.Response response) {
                gotResponse.set(response.isClosed());
            }

            @Override
            public void onFailure(Exception e) {
                assertNull(e.getMessage(), e);
            }
        });

        assertTrue(gotResponse.get());
    }

    public void testBuildWaitForCloseRequest() {
        List<String> openJobIds = Arrays.asList("openjob1", "openjob2");
        List<String> closingJobIds = Collections.singletonList("closingjob1");

        PersistentTasksCustomMetadata.Builder tasksBuilder =  PersistentTasksCustomMetadata.builder();
        addJobTask("openjob1", null, JobState.OPENED, tasksBuilder);
        addJobTask("openjob2", null, JobState.OPENED, tasksBuilder);
        addJobTask("closingjob1", null, JobState.CLOSING, tasksBuilder);

        TransportCloseJobAction.WaitForCloseRequest waitForCloseRequest =
            TransportCloseJobAction.buildWaitForCloseRequest(
                openJobIds, closingJobIds, tasksBuilder.build(), mock(AnomalyDetectionAuditor.class));
        assertEquals(waitForCloseRequest.jobsToFinalize, Arrays.asList("openjob1", "openjob2"));
        assertThat(waitForCloseRequest.persistentTasks, containsInAnyOrder(
            hasProperty("id", equalTo("job-openjob1")),
            hasProperty("id", equalTo("job-openjob2")),
            hasProperty("id", equalTo("job-closingjob1"))));
        assertTrue(waitForCloseRequest.hasJobsToWaitFor());

        waitForCloseRequest = TransportCloseJobAction.buildWaitForCloseRequest(Collections.emptyList(), Collections.emptyList(),
                tasksBuilder.build(), mock(AnomalyDetectionAuditor.class));
        assertFalse(waitForCloseRequest.hasJobsToWaitFor());
    }

    public static void addTask(String datafeedId, long startTime, String nodeId, DatafeedState state,
                               PersistentTasksCustomMetadata.Builder tasks) {
        tasks.addTask(MlTasks.datafeedTaskId(datafeedId), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(datafeedId, startTime), new Assignment(nodeId, "test assignment"));
        tasks.updateTaskState(MlTasks.datafeedTaskId(datafeedId), state);
    }

    private TransportCloseJobAction createAction() {
        return new TransportCloseJobAction(mock(TransportService.class), client, mock(ThreadPool.class),
            mock(ActionFilters.class), clusterService, mock(AnomalyDetectionAuditor.class), mock(PersistentTasksService.class),
            jobConfigProvider, datafeedConfigProvider);
    }

    @SuppressWarnings("unchecked")
    private void mockDatafeedConfigFindDatafeeds(Set<String> datafeedIds) {
        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(datafeedIds);

            return null;
        }).when(datafeedConfigProvider).findDatafeedIdsForJobIds(any(), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    private void mockJobConfigProviderExpandIds(Set<String> expandedIds) {
        doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[5];
            listener.onResponse(expandedIds);

            return null;
        }).when(jobConfigProvider).expandJobsIds(any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    private void mockClientStopDatafeed() {
        doAnswer(invocation -> {
            ActionListener<StopDatafeedAction.Response> listener =
                (ActionListener<StopDatafeedAction.Response>) invocation.getArguments()[2];
            listener.onResponse(new StopDatafeedAction.Response(true));

            return null;
        }).when(client).execute(same(StopDatafeedAction.INSTANCE), any(StopDatafeedAction.Request.class), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    private void mockClientIsolateDatafeed() {
        doAnswer(invocation -> {
            ActionListener<IsolateDatafeedAction.Response> listener =
                (ActionListener<IsolateDatafeedAction.Response>) invocation.getArguments()[2];
            listener.onResponse(new IsolateDatafeedAction.Response(true));

            return null;
        }).when(client).execute(same(IsolateDatafeedAction.INSTANCE), any(IsolateDatafeedAction.Request.class), any(ActionListener.class));
    }
}
