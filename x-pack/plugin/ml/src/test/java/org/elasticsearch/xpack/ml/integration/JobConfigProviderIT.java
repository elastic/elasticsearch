/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class JobConfigProviderIT extends MlSingleNodeTestCase {

    private JobConfigProvider jobConfigProvider;

    @Before
    public void createComponents() throws Exception {
        jobConfigProvider = new JobConfigProvider(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testGetMissingJob() throws InterruptedException {
        AtomicReference<Job.Builder> jobHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> jobConfigProvider.getJob("missing", actionListener), jobHolder, exceptionHolder);

        assertNull(jobHolder.get());
        assertNotNull(exceptionHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
    }

    public void testCheckJobExists() throws InterruptedException {
        AtomicReference<Boolean> jobExistsHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        boolean throwIfMissing = randomBoolean();
        blockingCall(actionListener ->
                jobConfigProvider.jobExists("missing", throwIfMissing, actionListener), jobExistsHolder, exceptionHolder);

        if (throwIfMissing) {
            assertNull(jobExistsHolder.get());
            assertNotNull(exceptionHolder.get());
            assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
        } else {
            assertFalse(jobExistsHolder.get());
            assertNull(exceptionHolder.get());
        }

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();

        // Create job
        Job job = createJob("existing-job", null).build(new Date());
        blockingCall(actionListener -> jobConfigProvider.putJob(job, actionListener), indexResponseHolder, exceptionHolder);

        exceptionHolder.set(null);
        blockingCall(actionListener ->
                jobConfigProvider.jobExists("existing-job", throwIfMissing, actionListener), jobExistsHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertNotNull(jobExistsHolder.get());
        assertTrue(jobExistsHolder.get());
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
        assertThat(exceptionHolder.get(), instanceOf(ResourceAlreadyExistsException.class));
        assertEquals("The job cannot be created with the Id 'same-id'. The Id is already used.", exceptionHolder.get().getMessage());
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
        blockingCall(actionListener -> jobConfigProvider.updateJob
                (jobId, jobUpdate, new ByteSizeValue(32), actionListener), updateJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals("This job has been updated", updateJobResponseHolder.get().getDescription());

        getJobResponseHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals("This job has been updated", getJobResponseHolder.get().build().getDescription());

        // Delete Job
        AtomicReference<DeleteResponse> deleteJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.deleteJob(jobId, true, actionListener),
                deleteJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(deleteJobResponseHolder.get().getResult(), equalTo(DocWriteResponse.Result.DELETED));

        // Read deleted job
        getJobResponseHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(getJobResponseHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());

        // Delete deleted job
        deleteJobResponseHolder.set(null);
        exceptionHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.deleteJob(jobId, true, actionListener),
                deleteJobResponseHolder, exceptionHolder);
        assertNull(deleteJobResponseHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());

        // and again with errorIfMissing set false
        deleteJobResponseHolder.set(null);
        exceptionHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.deleteJob(jobId, false, actionListener),
                deleteJobResponseHolder, exceptionHolder);
        assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteJobResponseHolder.get().getResult());
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
        blockingCall(actionListener -> jobConfigProvider.updateJob(jobId, invalidUpdate, new ByteSizeValue(32),
            actionListener), updateJobResponseHolder, exceptionHolder);
        assertNull(updateJobResponseHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("Invalid detector rule:"));
    }

    public void testUpdateWithValidator() throws Exception {
        final String jobId = "job-update-with-validator";

        // Create job
        Job newJob = createJob(jobId, null).build(new Date());
        this.<IndexResponse>blockingCall(actionListener -> jobConfigProvider.putJob(newJob, actionListener));

        JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setDescription("This job has been updated").build();

        JobConfigProvider.UpdateValidator validator = (job, update, listener) -> {
            listener.onResponse(null);
        };

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Job> updateJobResponseHolder = new AtomicReference<>();
        // update with the no-op validator
        blockingCall(actionListener -> jobConfigProvider.updateJobWithValidation(
            jobId, jobUpdate, new ByteSizeValue(32), validator, actionListener), updateJobResponseHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertNotNull(updateJobResponseHolder.get());
        assertEquals("This job has been updated", updateJobResponseHolder.get().getDescription());

        JobConfigProvider.UpdateValidator validatorWithAnError = (job, update, listener) -> {
            listener.onFailure(new IllegalStateException("I don't like this update"));
        };

        updateJobResponseHolder.set(null);
        // Update with a validator that errors
        blockingCall(actionListener -> jobConfigProvider.updateJobWithValidation(jobId, jobUpdate, new ByteSizeValue(32),
                validatorWithAnError, actionListener),
                updateJobResponseHolder, exceptionHolder);

        assertNull(updateJobResponseHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(IllegalStateException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("I don't like this update"));
    }

    public void testAllowNoJobs() throws InterruptedException {
        AtomicReference<SortedSet<String>> jobIdsHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> jobConfigProvider.expandJobsIds("_all", false, true, actionListener),
                jobIdsHolder, exceptionHolder);

        assertNull(jobIdsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
        assertThat(exceptionHolder.get().getMessage(), containsString("No known job with id"));

        exceptionHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.expandJobsIds("_all", true, false, actionListener),
                jobIdsHolder, exceptionHolder);
        assertNotNull(jobIdsHolder.get());
        assertNull(exceptionHolder.get());

        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", false, true, actionListener),
                jobsHolder, exceptionHolder);

        assertNull(jobsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
        assertThat(exceptionHolder.get().getMessage(), containsString("No known job with id"));

        exceptionHolder.set(null);
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", true, true, actionListener),
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
        SortedSet<String> expandedIds = blockingCall(actionListener ->
                jobConfigProvider.expandJobsIds("_all", true, false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "dick", "harry", "harry-jnr")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("*", true, true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "dick", "harry", "harry-jnr")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("tom,harry", true, false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("tom", "harry")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("harry-group,tom", true, false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("harry", "harry-jnr", "tom")), expandedIds);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<SortedSet<String>> jobIdsHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.expandJobsIds("tom,missing1,missing2", true, false, actionListener),
                jobIdsHolder, exceptionHolder);
        assertNull(jobIdsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
        assertThat(exceptionHolder.get().getMessage(), equalTo("No known job with id 'missing1,missing2'"));

        // Job builders
        List<Job.Builder> expandedJobsBuilders = blockingCall(actionListener ->
                jobConfigProvider.expandJobs("harry-group,tom", false, true, actionListener));
        List<Job> expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(harry, harryJnr, tom));

        expandedJobsBuilders = blockingCall(actionListener ->
                jobConfigProvider.expandJobs("_all", false, true, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(tom, dick, harry, harryJnr));

        expandedJobsBuilders = blockingCall(actionListener ->
                jobConfigProvider.expandJobs("tom,harry", false, false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(tom, harry));

        expandedJobsBuilders = blockingCall(actionListener ->
                jobConfigProvider.expandJobs("", false, false, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(tom, dick, harry, harryJnr));

        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.expandJobs("tom,missing1,missing2", false, true, actionListener),
                jobsHolder, exceptionHolder);
        assertNull(jobsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
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
        SortedSet<String> expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("foo*", true, true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("*-1", true, true,actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "foo-1")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("bar*", true, true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "bar-2", "nbar")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("b*r-1", true, true, actionListener));
        assertEquals(new TreeSet<>(Collections.singletonList("bar-1")), expandedIds);

        // Test full job config
        List<Job.Builder> expandedJobsBuilders =
                blockingCall(actionListener -> jobConfigProvider.expandJobs("foo*", true, true, actionListener));
        List<Job> expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(foo1, foo2));

        expandedJobsBuilders = blockingCall(actionListener -> jobConfigProvider.expandJobs("*-1", true, true, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(foo1, bar1));

        expandedJobsBuilders = blockingCall(actionListener -> jobConfigProvider.expandJobs("bar*", true, true, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(bar1, bar2, nbar));

        expandedJobsBuilders = blockingCall(actionListener -> jobConfigProvider.expandJobs("b*r-1", true, true, actionListener));
        expandedJobs = expandedJobsBuilders.stream().map(Job.Builder::build).collect(Collectors.toList());
        assertThat(expandedJobs, containsInAnyOrder(bar1));
    }

    public void testExpandJobIds_excludeDeleting() throws Exception {
        putJob(createJob("foo-1", null));
        putJob(createJob("foo-2", null));
        putJob(createJob("foo-deleting", null));
        putJob(createJob("bar", null));

        Boolean marked = blockingCall(actionListener -> jobConfigProvider.markJobAsDeleting("foo-deleting", actionListener));
        assertTrue(marked);

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        SortedSet<String> expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("foo*", true, true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("foo*", true, false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2", "foo-deleting")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("*", true, true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2", "bar")), expandedIds);

        expandedIds = blockingCall(actionListener -> jobConfigProvider.expandJobsIds("*", true, false, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2", "foo-deleting", "bar")), expandedIds);

        List<Job.Builder> expandedJobsBuilders =
                blockingCall(actionListener -> jobConfigProvider.expandJobs("foo*", true, true, actionListener));
        assertThat(expandedJobsBuilders, hasSize(2));

        expandedJobsBuilders = blockingCall(actionListener -> jobConfigProvider.expandJobs("foo*", true, false, actionListener));
        assertThat(expandedJobsBuilders, hasSize(3));
    }

    public void testExpandGroups() throws Exception {
        putJob(createJob("apples", Collections.singletonList("fruit")));
        putJob(createJob("pears", Collections.singletonList("fruit")));
        putJob(createJob("broccoli", Collections.singletonList("veg")));
        putJob(createJob("potato", Collections.singletonList("veg")));
        putJob(createJob("tomato", Arrays.asList("fruit", "veg")));
        putJob(createJob("unrelated", Collections.emptyList()));

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        SortedSet<String> expandedIds = blockingCall(actionListener ->
                jobConfigProvider.expandGroupIds(Collections.singletonList("fruit"), actionListener));
        assertThat(expandedIds, contains("apples", "pears", "tomato"));

        expandedIds = blockingCall(actionListener ->
                jobConfigProvider.expandGroupIds(Collections.singletonList("veg"), actionListener));
        assertThat(expandedIds, contains("broccoli", "potato", "tomato"));

        expandedIds = blockingCall(actionListener ->
                jobConfigProvider.expandGroupIds(Arrays.asList("fruit", "veg"), actionListener));
        assertThat(expandedIds, contains("apples", "broccoli", "pears", "potato", "tomato"));

        expandedIds = blockingCall(actionListener ->
                jobConfigProvider.expandGroupIds(Collections.singletonList("unknown-group"), actionListener));
        assertThat(expandedIds, empty());
    }

    public void testFindJobsWithCustomRules_GivenNoJobs() throws Exception {
        List<Job> foundJobs = blockingCall(listener -> jobConfigProvider.findJobsWithCustomRules(listener));
        assertThat(foundJobs.isEmpty(), is(true));
    }

    public void testFindJobsWithCustomRules() throws Exception {
        putJob(createJob("job-without-rules", Collections.emptyList()));

        DetectionRule rule = new DetectionRule.Builder(Collections.singletonList(
                new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 0.0))).build();

        Job.Builder jobWithRules1 = createJob("job-with-rules-1", Collections.emptyList());
        jobWithRules1 = addCustomRule(jobWithRules1, rule);
        putJob(jobWithRules1);
        Job.Builder jobWithRules2 = createJob("job-with-rules-2", Collections.emptyList());
        jobWithRules2 = addCustomRule(jobWithRules2, rule);
        putJob(jobWithRules2);

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        List<Job> foundJobs = blockingCall(listener -> jobConfigProvider.findJobsWithCustomRules(listener));

        Set<String> foundJobIds = foundJobs.stream().map(Job::getId).collect(Collectors.toSet());
        assertThat(foundJobIds.size(), equalTo(2));
        assertThat(foundJobIds, containsInAnyOrder(jobWithRules1.getId(), jobWithRules2.getId()));
    }

    public void testValidateDatafeedJob() throws Exception {
        String jobId = "validate-df-job";
        putJob(createJob(jobId, Collections.emptyList()));

        AtomicReference<Boolean> responseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df1", jobId);
        builder.setIndices(Collections.singletonList("data-index"));
        DatafeedConfig config = builder.build();

        blockingCall(listener -> jobConfigProvider.validateDatafeedJob(config, listener), responseHolder, exceptionHolder);
        assertTrue(responseHolder.get());
        assertNull(exceptionHolder.get());

        builder = new DatafeedConfig.Builder("df1", jobId);
        builder.setIndices(Collections.singletonList("data-index"));

        // This config is not valid because it uses aggs but the job's
        // summary count field is not set
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        HistogramAggregationBuilder histogram =
                AggregationBuilders.histogram("time").interval(1800.0).field("time").subAggregation(maxTime);
        builder.setParsedAggregations(new AggregatorFactories.Builder().addAggregator(histogram));
        DatafeedConfig badConfig = builder.build();

        blockingCall(listener -> jobConfigProvider.validateDatafeedJob(badConfig, listener), responseHolder, exceptionHolder);
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ElasticsearchStatusException.class));
        assertEquals(Messages.DATAFEED_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD, exceptionHolder.get().getMessage());
    }

    public void testMarkAsDeleting() throws Exception {
        AtomicReference<Boolean> responseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> jobConfigProvider.markJobAsDeleting("missing-job", listener), responseHolder, exceptionHolder);
        assertNull(responseHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());

        String jobId = "mark-as-deleting-job";
        putJob(createJob(jobId, Collections.emptyList()));
        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        exceptionHolder.set(null);
        blockingCall(listener -> jobConfigProvider.markJobAsDeleting(jobId, listener), responseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertTrue(responseHolder.get());

        // repeat the update for good measure
        blockingCall(listener -> jobConfigProvider.markJobAsDeleting(jobId, listener), responseHolder, exceptionHolder);
        assertTrue(responseHolder.get());
        assertNull(exceptionHolder.get());
    }

    private static Job.Builder createJob(String jobId, List<String> groups) {
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

    private static Job.Builder addCustomRule(Job.Builder job, DetectionRule rule) {
        JobUpdate.Builder update1 = new JobUpdate.Builder(job.getId());
        update1.setDetectorUpdates(Collections.singletonList(new JobUpdate.DetectorUpdate(0, null, Collections.singletonList(rule))));
        Job updatedJob = update1.build().mergeWithJob(job.build(new Date()), null);
        return new Job.Builder(updatedJob);
    }

    private Job putJob(Job.Builder job) throws Exception {
        Job builtJob = job.build(new Date());
        this.<IndexResponse>blockingCall(actionListener -> jobConfigProvider.putJob(builtJob, actionListener));
        return builtJob;
    }
}
