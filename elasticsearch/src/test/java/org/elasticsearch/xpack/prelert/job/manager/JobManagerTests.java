/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.job.metadata.PrelertMetadata;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyString;
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
        PrelertMetadata.Builder builder = new PrelertMetadata.Builder();
        builder.putJob(buildJobBuilder("foo").build(), false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("name"))
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, builder.build())).build();
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
                .assignToNode("foo", "nodeId")
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

        result = jobManager.getJobs(0, 5, clusterState);
        assertThat(result.count(), equalTo(10L));
        assertThat(result.results().get(0).getId(), equalTo("0"));
        assertThat(result.results().get(1).getId(), equalTo("1"));
        assertThat(result.results().get(2).getId(), equalTo("2"));
        assertThat(result.results().get(3).getId(), equalTo("3"));
        assertThat(result.results().get(4).getId(), equalTo("4"));

        result = jobManager.getJobs(5, 5, clusterState);
        assertThat(result.count(), equalTo(10L));
        assertThat(result.results().get(0).getId(), equalTo("5"));
        assertThat(result.results().get(1).getId(), equalTo("6"));
        assertThat(result.results().get(2).getId(), equalTo("7"));
        assertThat(result.results().get(3).getId(), equalTo("8"));
        assertThat(result.results().get(4).getId(), equalTo("9"));

        result = jobManager.getJobs(9, 1, clusterState);
        assertThat(result.count(), equalTo(10L));
        assertThat(result.results().get(0).getId(), equalTo("9"));

        result = jobManager.getJobs(9, 10, clusterState);
        assertThat(result.count(), equalTo(10L));
        assertThat(result.results().get(0).getId(), equalTo("9"));
    }

    private JobManager createJobManager() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        JobResultsPersister jobResultsPersister = mock(JobResultsPersister.class);
        return new JobManager(settings, jobProvider, jobResultsPersister, clusterService);
    }

    private ClusterState createClusterState() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("_name"));
        builder.metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, PrelertMetadata.PROTO));
        return builder.build();
    }
}
