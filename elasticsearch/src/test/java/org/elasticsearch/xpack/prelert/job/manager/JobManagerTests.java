/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.manager;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.metadata.PrelertMetadata;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobManagerTests extends ESTestCase {

    private ClusterService clusterService;
    private JobProvider jobProvider;
    private JobDataCountsPersister jobDataCountsPersister;
    private Auditor auditor;

    @Before
    public void setupMocks() {
        clusterService = mock(ClusterService.class);
        jobProvider = mock(JobProvider.class);
        jobDataCountsPersister = mock(JobDataCountsPersister.class);
        auditor = mock(Auditor.class);
        when(jobProvider.audit(anyString())).thenReturn(auditor);
    }

    public void testGetJob() {
        JobManager jobManager = createJobManager();
        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();
        builder.putJob(buildJobBuilder("foo").build(), false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("name"))
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, builder.build())).build();
        QueryPage<Job> doc = jobManager.getJob("foo", clusterState);
        assertTrue(doc.hitCount() > 0);
        assertThat(doc.hits().get(0).getJobId(), equalTo("foo"));
    }

    public void testFilter() {
        Set<String> running = new HashSet<String>(Arrays.asList("henry", "dim", "dave"));
        Set<String> diff = new HashSet<>(Arrays.asList("dave", "tom")).stream().filter((s) -> !running.contains(s))
                .collect(Collectors.toCollection(HashSet::new));

        assertTrue(diff.size() == 1);
        assertTrue(diff.contains("tom"));
    }

    public void testRemoveJobFromClusterState_GivenExistingMetadata() {
        JobManager jobManager = createJobManager();
        ClusterState clusterState = createClusterState();
        Job job = buildJobBuilder("foo").build();
        clusterState = jobManager.innerPutJob(job, false, clusterState);

        clusterState = jobManager.removeJobFromClusterState("foo", clusterState);

        PrelertMetadata prelertMetadata = clusterState.metaData().custom(PrelertMetadata.TYPE);
        assertThat(prelertMetadata.getJobs().containsKey("foo"), is(false));
    }

    public void testRemoveJobFromClusterState_GivenJobIsRunning() {
        JobManager jobManager = createJobManager();
        ClusterState clusterState = createClusterState();
        Job job = buildJobBuilder("foo").build();
        clusterState = jobManager.innerPutJob(job, false, clusterState);
        Allocation.Builder allocation = new Allocation.Builder();
        allocation.setNodeId("myNode");
        allocation.setJobId(job.getId());
        allocation.setStatus(JobStatus.RUNNING);
        PrelertMetadata.Builder newMetadata = new PrelertMetadata.Builder(clusterState.metaData().custom(PrelertMetadata.TYPE));
        newMetadata.putAllocation("myNode", job.getId());
        newMetadata.updateAllocation(job.getId(), allocation.build());

        ClusterState jobRunningClusterState = new ClusterState.Builder(clusterState)
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, newMetadata.build())).build();

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> jobManager.removeJobFromClusterState("foo", jobRunningClusterState));
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        assertThat(e.getMessage(), equalTo("Cannot delete job 'foo' while it is RUNNING"));
    }

    public void testRemoveJobFromClusterState_jobMissing() {
        JobManager jobManager = createJobManager();
        ClusterState clusterState = createClusterState();
        Job job = buildJobBuilder("foo").build();
        ClusterState clusterState2 = jobManager.innerPutJob(job, false, clusterState);
        Exception e = expectThrows(ResourceNotFoundException.class, () -> jobManager.removeJobFromClusterState("bar", clusterState2));
        assertThat(e.getMessage(), equalTo("job [bar] does not exist"));
    }

    public void testGetJobOrThrowIfUnknown_GivenUnknownJob() {
        JobManager jobManager = createJobManager();
        ClusterState cs = createClusterState();
        ESTestCase.expectThrows(ResourceNotFoundException.class, () -> jobManager.getJobOrThrowIfUnknown(cs, "foo"));
    }

    public void testGetJobOrThrowIfUnknown_GivenKnownJob() {
        JobManager jobManager = createJobManager();
        Job job = buildJobBuilder("foo").build();
        PrelertMetadata prelertMetadata = new PrelertMetadata.Builder().putJob(job, false).build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, prelertMetadata)).build();

        assertEquals(job, jobManager.getJobOrThrowIfUnknown(cs, "foo"));
    }

    public void tesGetJobAllocation() {
        JobManager jobManager = createJobManager();
        Job job = buildJobBuilder("foo").build();
        PrelertMetadata prelertMetadata = new PrelertMetadata.Builder()
                .putJob(job, false)
                .putAllocation("nodeId", "foo")
                .build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, prelertMetadata)).build();
        when(clusterService.state()).thenReturn(cs);

        assertEquals("nodeId", jobManager.getJobAllocation("foo").getNodeId());
        expectThrows(ResourceNotFoundException.class, () -> jobManager.getJobAllocation("bar"));
    }

    public void testGetJobs() {
        PrelertMetadata.Builder prelertMetadata = new PrelertMetadata.Builder();
        for (int i = 0; i < 10; i++) {
            prelertMetadata.putJob(buildJobBuilder(Integer.toString(i)).build(), false);
        }
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, prelertMetadata.build())).build();

        JobManager jobManager = createJobManager();
        QueryPage<Job> result = jobManager.getJobs(0, 10, clusterState);
        assertThat(result.hitCount(), equalTo(10L));
        assertThat(result.hits().get(0).getId(), equalTo("0"));
        assertThat(result.hits().get(1).getId(), equalTo("1"));
        assertThat(result.hits().get(2).getId(), equalTo("2"));
        assertThat(result.hits().get(3).getId(), equalTo("3"));
        assertThat(result.hits().get(4).getId(), equalTo("4"));
        assertThat(result.hits().get(5).getId(), equalTo("5"));
        assertThat(result.hits().get(6).getId(), equalTo("6"));
        assertThat(result.hits().get(7).getId(), equalTo("7"));
        assertThat(result.hits().get(8).getId(), equalTo("8"));
        assertThat(result.hits().get(9).getId(), equalTo("9"));

        result = jobManager.getJobs(0, 5, clusterState);
        assertThat(result.hitCount(), equalTo(10L));
        assertThat(result.hits().get(0).getId(), equalTo("0"));
        assertThat(result.hits().get(1).getId(), equalTo("1"));
        assertThat(result.hits().get(2).getId(), equalTo("2"));
        assertThat(result.hits().get(3).getId(), equalTo("3"));
        assertThat(result.hits().get(4).getId(), equalTo("4"));

        result = jobManager.getJobs(5, 5, clusterState);
        assertThat(result.hitCount(), equalTo(10L));
        assertThat(result.hits().get(0).getId(), equalTo("5"));
        assertThat(result.hits().get(1).getId(), equalTo("6"));
        assertThat(result.hits().get(2).getId(), equalTo("7"));
        assertThat(result.hits().get(3).getId(), equalTo("8"));
        assertThat(result.hits().get(4).getId(), equalTo("9"));

        result = jobManager.getJobs(9, 1, clusterState);
        assertThat(result.hitCount(), equalTo(10L));
        assertThat(result.hits().get(0).getId(), equalTo("9"));

        result = jobManager.getJobs(9, 10, clusterState);
        assertThat(result.hitCount(), equalTo(10L));
        assertThat(result.hits().get(0).getId(), equalTo("9"));
    }

    public void testInnerPutJob() {
        JobManager jobManager = createJobManager();
        ClusterState cs = createClusterState();

        Job job1 = buildJobBuilder("_id").build();
        ClusterState result1 = jobManager.innerPutJob(job1, false, cs);
        PrelertMetadata pm = result1.getMetaData().custom(PrelertMetadata.TYPE);
        assertThat(pm.getJobs().get("_id"), sameInstance(job1));

        Job job2 = buildJobBuilder("_id").build();
        expectThrows(ResourceAlreadyExistsException.class, () -> jobManager.innerPutJob(job2, false, result1));

        ClusterState result2 = jobManager.innerPutJob(job2, true, result1);
        pm = result2.getMetaData().custom(PrelertMetadata.TYPE);
        assertThat(pm.getJobs().get("_id"), sameInstance(job2));
    }

    private JobManager createJobManager() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = new Environment(
                settings);
        return new JobManager(env, settings, jobProvider, jobDataCountsPersister, clusterService);
    }

    private ClusterState createClusterState() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("_name"));
        builder.metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, PrelertMetadata.PROTO));
        return builder.build();
    }
}
