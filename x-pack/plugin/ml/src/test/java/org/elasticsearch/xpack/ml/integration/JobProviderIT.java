package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class JobProviderIT extends MlSingleNodeTestCase {

    private JobProvider jobProvider;

    @Before
    public void createComponents() throws Exception {
        jobProvider = new JobProvider(client(), Settings.EMPTY);
        waitForMlTemplates();
    }

    public void testGetMissingJob() throws InterruptedException {
        AtomicReference<Job.Builder> jobHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> jobProvider.getJob("missing", actionListener), jobHolder, exceptionHolder);

        assertNull(jobHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testCrud() throws InterruptedException {
        final String jobId = "crud-job";

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create job
        Job newJob = createJob(jobId, null).build(new Date());
        blockingCall(actionListener -> jobProvider.putJob(newJob, actionListener), indexResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertNotNull(indexResponseHolder.get());

        // Read Job
        AtomicReference<Job.Builder> getJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobProvider.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());

        assertEquals(newJob, getJobResponseHolder.get().build());

        // Update Job
        indexResponseHolder.set(null);
        JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setDescription("This job has been updated").build();

        Function<Job.Builder, Job> jobUpdater = jobBuilder -> {
            Job currentJob = jobBuilder.build();
            return jobUpdate.mergeWithJob(currentJob, new ByteSizeValue(1024));
        };

        AtomicReference<Job> updateJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobProvider.updateJob(jobId, jobUpdater, actionListener),
                updateJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals("This job has been updated", updateJobResponseHolder.get().getDescription());

        getJobResponseHolder.set(null);
        blockingCall(actionListener -> jobProvider.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals("This job has been updated", getJobResponseHolder.get().build().getDescription());

        // Delete Job
        AtomicReference<Boolean> deleteJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobProvider.deleteJob(jobId, actionListener),
                deleteJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertTrue(deleteJobResponseHolder.get());

        // Read deleted job
        getJobResponseHolder.set(null);
        blockingCall(actionListener -> jobProvider.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(getJobResponseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testExpandJobs_GroupsAndJobIds() throws Exception {
        Job tom = putJob(createJob("tom", null));
        Job dick = putJob(createJob("dick", null));
        Job harry = putJob(createJob("harry", Collections.singletonList("harry-group")));
        Job harryJnr = putJob(createJob("harry-jnr", Collections.singletonList("harry-group")));

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        Set<String> expandedIds = blockingCall(actionListener -> jobProvider.expandJobsIds("_all", false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "dick", "harry", "harry-jnr")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobProvider.expandJobsIds("*", false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "dick", "harry", "harry-jnr")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobProvider.expandJobsIds("tom,harry", false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "harry")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobProvider.expandJobsIds("harry-group,tom", false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("harry", "harry-jnr", "tom")), expandedIds);

        List<Job.Builder> expandedJobsBuilders = blockingCall(actionListener ->
                jobProvider.expandJobs("harry-group,tom", false, actionListener));
        List<Job> expandedJobs = expandedJobsBuilders.stream().map(j ->  j.build()).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(harry, harryJnr, tom));

        expandedJobsBuilders = blockingCall(actionListener ->
                jobProvider.expandJobs("_all", false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(j ->  j.build()).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(tom, dick, harry, harryJnr));
    }

    public void testExpandJobs_WildCardExpansion() throws Exception {

        Job foo1 = putJob(createJob("foo-1", null));
        Job foo2 = putJob(createJob("foo-2", null));
        Job bar1 = putJob(createJob("bar-1", Collections.singletonList("bar")));
        Job bar2 = putJob(createJob("bar-2", Collections.singletonList("bar")));
        Job nbar = putJob(createJob("nbar", Collections.singletonList("bar")));

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        // Test job IDs only
        Set<String> expandedIds = blockingCall(actionListener -> jobProvider.expandJobsIds("foo*", false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobProvider.expandJobsIds("*-1", false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "foo-1")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobProvider.expandJobsIds("bar*", false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "bar-2", "nbar")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobProvider.expandJobsIds("b*r-1", false, actionListener));
        assertEquals(new TreeSet<>(Collections.singletonList("bar-1")), expandedIds);

        // Test full job config
        List<Job.Builder> expandedJobsBuilders = blockingCall(actionListener -> jobProvider.expandJobs("foo*", false, actionListener));
        List<Job> expandedJobs = expandedJobsBuilders.stream().map(j ->  j.build()).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(foo1, foo2));

        expandedJobsBuilders = blockingCall(actionListener -> jobProvider.expandJobs("*-1", false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(j ->  j.build()).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(foo1, bar1));

        expandedJobsBuilders = blockingCall(actionListener -> jobProvider.expandJobs("bar*", false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(j ->  j.build()).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(bar1, bar2, nbar));

        expandedJobsBuilders = blockingCall(actionListener -> jobProvider.expandJobs("b*r-1", false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(j ->  j.build()).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(bar1));
    }

    private Job.Builder createJob(String jobId, List<String> groups) {
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("client");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d1.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId(jobId);
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(new DataDescription.Builder());
        if (groups != null && groups.isEmpty() == false) {
            builder.setGroups(groups);
        }
        return builder;
    }

    private <T> void blockingCall(Consumer<ActionListener<T>> function, AtomicReference<T> response,
                                  AtomicReference<Exception> error) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(
                r -> {
                    response.set(r);
                    latch.countDown();
                },
                e -> {
                    error.set(e);
                    latch.countDown();
                }
        );

        function.accept(listener);
        latch.await();
    }

    private <T> T blockingCall(Consumer<ActionListener<T>> function) throws Exception {
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<T> responseHolder = new AtomicReference<>();
        blockingCall(function, responseHolder, exceptionHolder);
        if (exceptionHolder.get() != null) {
            assertNotNull(exceptionHolder.get().getMessage(), exceptionHolder.get());
        }
        return responseHolder.get();
    }

    private Job putJob(Job.Builder job) throws Exception {
        Job builtJob = job.build(new Date());
        this.<IndexResponse>blockingCall(actionListener -> jobProvider.putJob(builtJob, actionListener));
        return builtJob;
    }
}
