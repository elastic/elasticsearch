/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class JobConfigProviderIT extends MlSingleNodeTestCase {

    private JobConfigProvider jobConfigProvider;

    @Before
    public void createComponents() throws Exception {
        jobConfigProvider = new JobConfigProvider(client(), Settings.EMPTY);
        waitForMlTemplates();
    }

    public void testGetMissingJob() throws InterruptedException {
        AtomicReference<Job.Builder> jobHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> jobConfigProvider.getJob("missing", actionListener), jobHolder, exceptionHolder);

        assertNull(jobHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testOverwriteNotAllowed() throws InterruptedException {
        final String jobId = "same-id";

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create job
        Job initialJob = createJob(jobId, null).build(new Date());
        blockingCall(actionListener -> jobConfigProvider.putJob(initialJob, actionListener), indexResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertNotNull(indexResponseHolder.get());

        indexResponseHolder.set(null);
        Job jobWithSameId = createJob(jobId, null).build(new Date());
        blockingCall(actionListener -> jobConfigProvider.putJob(jobWithSameId, actionListener), indexResponseHolder, exceptionHolder);
        assertNull(indexResponseHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(VersionConflictEngineException.class));
    }

    public void testCrud() throws InterruptedException {
        final String jobId = "crud-job";

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create job
        Job newJob = createJob(jobId, null).build(new Date());
        blockingCall(actionListener -> jobConfigProvider.putJob(newJob, actionListener), indexResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertNotNull(indexResponseHolder.get());

        // Read Job
        AtomicReference<Job.Builder> getJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());

        assertEquals(newJob, getJobResponseHolder.get().build());

        // Update Job
        indexResponseHolder.set(null);
        JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setDescription("This job has been updated").build();

        AtomicReference<Job> updateJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.updateJob(jobId, jobUpdate, new ByteSizeValue(32), actionListener),
                updateJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals("This job has been updated", updateJobResponseHolder.get().getDescription());

        getJobResponseHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals("This job has been updated", getJobResponseHolder.get().build().getDescription());

        // Delete Job
        AtomicReference<DeleteResponse> deleteJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.deleteJob(jobId, actionListener),
                deleteJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(deleteJobResponseHolder.get().getResult(), equalTo(DocWriteResponse.Result.DELETED));

        // Read deleted job
        getJobResponseHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(getJobResponseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));

        // Delete deleted job
        deleteJobResponseHolder.set(null);
        exceptionHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.deleteJob(jobId, actionListener),
                deleteJobResponseHolder, exceptionHolder);
        assertNull(deleteJobResponseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testUpdateWithAValidationError() throws Exception {
        final String jobId = "bad-update-job";

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create job
        Job newJob = createJob(jobId, null).build(new Date());
        blockingCall(actionListener -> jobConfigProvider.putJob(newJob, actionListener), indexResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertNotNull(indexResponseHolder.get());

        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder().exclude("not a used field", "filerfoo")).build();
        JobUpdate.DetectorUpdate detectorUpdate = new JobUpdate.DetectorUpdate(0, null, Collections.singletonList(rule));
        JobUpdate invalidUpdate = new JobUpdate.Builder(jobId)
                .setDetectorUpdates(Collections.singletonList(detectorUpdate))
                .build();

        AtomicReference<Job> updateJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.updateJob(jobId, invalidUpdate, new ByteSizeValue(32), actionListener),
                updateJobResponseHolder, exceptionHolder);
        assertNull(updateJobResponseHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("Invalid detector rule:"));
    }

    public void testAllowNoJobs() throws InterruptedException {
        AtomicReference<Set<String>> jobIdsHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> jobConfigProvider.expandJobsIds("_all", false, actionListener),
                jobIdsHolder, exceptionHolder);

        assertNull(jobIdsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("No known job with id"));

        exceptionHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.expandJobsIds("_all", true, actionListener),
                jobIdsHolder, exceptionHolder);
        assertNotNull(jobIdsHolder.get());
        assertNull(exceptionHolder.get());

        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", false, actionListener),
                jobsHolder, exceptionHolder);

        assertNull(jobsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("No known job with id"));

        exceptionHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", true, actionListener),
                jobsHolder, exceptionHolder);
        assertNotNull(jobsHolder.get());
        assertNull(exceptionHolder.get());
    }

    public void testExpandJobs_GroupsAndJobIds() throws Exception {
        Job tom = putJob(createJob("tom", null));
        Job dick = putJob(createJob("dick", null));
        Job harry = putJob(createJob("harry", Collections.singletonList("harry-group")));
        Job harryJnr = putJob(createJob("harry-jnr", Collections.singletonList("harry-group")));

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        // Job Ids
        Set<String> expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("_all", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "dick", "harry", "harry-jnr")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("*", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "dick", "harry", "harry-jnr")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("tom,harry", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "harry")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("harry-group,tom", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("harry", "harry-jnr", "tom")), expandedIds);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Set<String>> jobIdsHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.expandJobsIds("tom,missing1,missing2", true, actionListener),
                jobIdsHolder, exceptionHolder);
        assertNull(jobIdsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
        assertThat(exceptionHolder.get().getMessage(), equalTo("No known job with id 'missing1,missing2'"));

        // Job builders
        List<Job.Builder> expandedJobsBuilders = blockingCall(actionListener ->
                jobConfigProvider.expandJobs("harry-group,tom", false, actionListener));
        List<Job> expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(harry, harryJnr, tom));

        expandedJobsBuilders = blockingCall(actionListener ->
                jobConfigProvider.expandJobs("_all", false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(tom, dick, harry, harryJnr));

        expandedJobsBuilders = blockingCall(actionListener ->
                jobConfigProvider.expandJobs("tom,harry", false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(tom, harry));

        expandedJobsBuilders = blockingCall(actionListener ->
                jobConfigProvider.expandJobs("", false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(tom, dick, harry, harryJnr));

        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.expandJobs("tom,missing1,missing2", false, actionListener),
                jobsHolder, exceptionHolder);
        assertNull(jobsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
        assertThat(exceptionHolder.get().getMessage(), equalTo("No known job with id 'missing1,missing2'"));
    }

    public void testExpandJobs_WildCardExpansion() throws Exception {
        Job foo1 = putJob(createJob("foo-1", null));
        Job foo2 = putJob(createJob("foo-2", null));
        Job bar1 = putJob(createJob("bar-1", Collections.singletonList("bar")));
        Job bar2 = putJob(createJob("bar-2", Collections.singletonList("bar")));
        Job nbar = putJob(createJob("nbar", Collections.singletonList("bar")));

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        // Test job IDs only
        Set<String> expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("foo*", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("*-1", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "foo-1")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("bar*", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "bar-2", "nbar")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("b*r-1", true, actionListener));
        assertEquals(new TreeSet<>(Collections.singletonList("bar-1")), expandedIds);

        // Test full job config
        List<Job.Builder> expandedJobsBuilders = blockingCall(actionListener -> jobConfigProvider.expandJobs("foo*", true, actionListener));
        List<Job> expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(foo1, foo2));

        expandedJobsBuilders = blockingCall(actionListener -> jobConfigProvider.expandJobs("*-1", true, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(foo1, bar1));

        expandedJobsBuilders = blockingCall(actionListener -> jobConfigProvider.expandJobs("bar*", true, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(bar1, bar2, nbar));

        expandedJobsBuilders = blockingCall(actionListener -> jobConfigProvider.expandJobs("b*r-1", true, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
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
        this.<IndexResponse>blockingCall(actionListener -> jobConfigProvider.putJob(builtJob, actionListener));
        return builtJob;
    }
}
