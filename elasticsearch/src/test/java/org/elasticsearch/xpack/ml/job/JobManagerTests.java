/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobManagerTests extends ESTestCase {

    private ClusterService clusterService;
    private JobProvider jobProvider;

    @Before
    public void setupMocks() {
        clusterService = mock(ClusterService.class);
        jobProvider = mock(JobProvider.class);
        Auditor auditor = mock(Auditor.class);
        when(jobProvider.audit(anyString())).thenReturn(auditor);
    }

    public void testGetJob() {
        JobManager jobManager = createJobManager();
        MlMetadata.Builder builder = new MlMetadata.Builder();
        builder.putJob(buildJobBuilder("foo").build(), false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, builder.build())).build();
        QueryPage<Job> doc = jobManager.getJob("foo", clusterState);
        assertTrue(doc.count() > 0);
        assertThat(doc.results().get(0).getId(), equalTo("foo"));
    }

    public void testFilter() {
        Set<String> running = new HashSet<>(Arrays.asList("henry", "dim", "dave"));
        Set<String> diff = new HashSet<>(Arrays.asList("dave", "tom")).stream().filter((s) -> !running.contains(s))
                .collect(Collectors.toCollection(HashSet::new));

        assertTrue(diff.size() == 1);
        assertTrue(diff.contains("tom"));
    }

    public void testGetJobOrThrowIfUnknown_GivenUnknownJob() {
        JobManager jobManager = createJobManager();
        ClusterState cs = createClusterState();
        ESTestCase.expectThrows(ResourceNotFoundException.class, () -> jobManager.getJobOrThrowIfUnknown(cs, "foo"));
    }

    public void testGetJobOrThrowIfUnknown_GivenKnownJob() {
        JobManager jobManager = createJobManager();
        Job job = buildJobBuilder("foo").build();
        MlMetadata mlMetadata = new MlMetadata.Builder().putJob(job, false).build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata)).build();

        assertEquals(job, jobManager.getJobOrThrowIfUnknown(cs, "foo"));
    }

    public void tesGetJobAllocation() {
        JobManager jobManager = createJobManager();
        Job job = buildJobBuilder("foo").build();
        MlMetadata mlMetadata = new MlMetadata.Builder()
                .putJob(job, false)
                .assignToNode("foo", "nodeId")
                .build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata)).build();
        when(clusterService.state()).thenReturn(cs);

        assertEquals("nodeId", jobManager.getJobAllocation("foo").getNodeId());
        expectThrows(ResourceNotFoundException.class, () -> jobManager.getJobAllocation("bar"));
    }

    public void testGetJob_GivenJobIdIsAll() {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        for (int i = 0; i < 3; i++) {
            mlMetadata.putJob(buildJobBuilder(Integer.toString(i)).build(), false);
        }
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata.build())).build();

        JobManager jobManager = createJobManager();
        QueryPage<Job> result = jobManager.getJob("_all", clusterState);
        assertThat(result.count(), equalTo(3L));
        assertThat(result.results().get(0).getId(), equalTo("0"));
        assertThat(result.results().get(1).getId(), equalTo("1"));
        assertThat(result.results().get(2).getId(), equalTo("2"));
    }

    public void testGetJobs() {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        for (int i = 0; i < 10; i++) {
            mlMetadata.putJob(buildJobBuilder(Integer.toString(i)).build(), false);
        }
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata.build())).build();

        JobManager jobManager = createJobManager();
        QueryPage<Job> result = jobManager.getJobs(clusterState);
        assertThat(result.count(), equalTo(10L));
        assertThat(result.results().get(0).getId(), equalTo("0"));
        assertThat(result.results().get(1).getId(), equalTo("1"));
        assertThat(result.results().get(2).getId(), equalTo("2"));
        assertThat(result.results().get(3).getId(), equalTo("3"));
        assertThat(result.results().get(4).getId(), equalTo("4"));
        assertThat(result.results().get(5).getId(), equalTo("5"));
        assertThat(result.results().get(6).getId(), equalTo("6"));
        assertThat(result.results().get(7).getId(), equalTo("7"));
        assertThat(result.results().get(8).getId(), equalTo("8"));
        assertThat(result.results().get(9).getId(), equalTo("9"));
    }

    @SuppressWarnings("unchecked")
    public void testPutJobFailsIfIndexExists() {
        JobManager jobManager = createJobManager();
        Job.Builder jobBuilder = buildJobBuilder("foo");
        jobBuilder.setIndexName("my-special-place");
        PutJobAction.Request request = new PutJobAction.Request(jobBuilder.build());

        Index index = mock(Index.class);
        when(index.getName()).thenReturn(AnomalyDetectorsIndex.jobResultsIndexName("my-special-place"));
        IndexMetaData indexMetaData = mock(IndexMetaData.class);
        when(indexMetaData.getIndex()).thenReturn(index);
        ImmutableOpenMap<String, AliasMetaData> aliases = ImmutableOpenMap.of();
        when(indexMetaData.getAliases()).thenReturn(aliases);

        ImmutableOpenMap<String, IndexMetaData> indexMap = ImmutableOpenMap.<String, IndexMetaData>builder()
                .fPut(AnomalyDetectorsIndex.jobResultsIndexName("my-special-place"), indexMetaData).build();

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, MlMetadata.EMPTY_METADATA).indices(indexMap)).build();

        doAnswer(invocationOnMock -> {
            AckedClusterStateUpdateTask<Boolean> task = (AckedClusterStateUpdateTask<Boolean>) invocationOnMock.getArguments()[1];
            task.execute(cs);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("put-job-foo"), any(AckedClusterStateUpdateTask.class));

        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class, () -> jobManager.putJob(request,
                new ActionListener<PutJobAction.Response>() {
            @Override
            public void onResponse(PutJobAction.Response response) {
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }
        }));

        assertEquals("Cannot create index '.ml-anomalies-my-special-place' as it already exists", e.getMessage());
    }

    private JobManager createJobManager() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        JobResultsPersister jobResultsPersister = mock(JobResultsPersister.class);
        return new JobManager(settings, jobProvider, jobResultsPersister, clusterService);
    }

    private ClusterState createClusterState() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("_name"));
        builder.metaData(MetaData.builder().putCustom(MlMetadata.TYPE, MlMetadata.EMPTY_METADATA));
        return builder.build();
    }
}
