/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.RuleAction;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCountsTests;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.persistence.CalendarQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.ScheduledEventsQueryBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;


public class JobResultsProviderIT extends MlSingleNodeTestCase {

    private JobResultsProvider jobProvider;
    private ResultsPersisterService resultsPersisterService;
    private AnomalyDetectionAuditor auditor;

    @Before
    public void createComponents() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        jobProvider = new JobResultsProvider(client(), builder.build());
        ThreadPool tp = mock(ThreadPool.class);
        ClusterSettings clusterSettings = new ClusterSettings(builder.build(),
            new HashSet<>(Arrays.asList(InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                ClusterService.USER_DEFINED_META_DATA,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING)));
        ClusterService clusterService = new ClusterService(builder.build(), clusterSettings, tp);

        resultsPersisterService = new ResultsPersisterService(client(), clusterService, builder.build());
        auditor = new AnomalyDetectionAuditor(client(), "test_node");
        waitForMlTemplates();
    }

    @AwaitsFix(bugUrl ="https://github.com/elastic/elasticsearch/issues/40134")
    public void testMultipleSimultaneousJobCreations() {

        int numJobs = randomIntBetween(4, 7);

        // Each job should result in one extra field being added to the results index mappings: field1, field2, field3, etc.
        // Due to all being created simultaneously this test may reveal race conditions in the code that updates the mappings.
        List<PutJobAction.Request> requests = new ArrayList<>(numJobs);
        for (int i = 1; i <= numJobs; ++i) {
            Job.Builder builder = new Job.Builder("job" + i);
            AnalysisConfig.Builder ac = createAnalysisConfig("field" + i, Collections.emptyList());
            DataDescription.Builder dc = new DataDescription.Builder();
            builder.setAnalysisConfig(ac);
            builder.setDataDescription(dc);

            requests.add(new PutJobAction.Request(builder));
        }

        // Start the requests as close together as possible, without waiting for each to complete before starting the next one.
        List<ActionFuture<PutJobAction.Response>> futures = new ArrayList<>(numJobs);
        for (PutJobAction.Request request : requests) {
            futures.add(client().execute(PutJobAction.INSTANCE, request));
        }

        // Only after all requests are in-flight, wait for all the requests to complete.
        for (ActionFuture<PutJobAction.Response> future : futures) {
            future.actionGet();
        }

        // Assert that the mappings contain all the additional fields: field1, field2, field3, etc.
        String sharedResultsIndex = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
        GetMappingsRequest request = new GetMappingsRequest().indices(sharedResultsIndex);
        GetMappingsResponse response = client().execute(GetMappingsAction.INSTANCE, request).actionGet();
        ImmutableOpenMap<String, MappingMetaData> indexMappings = response.getMappings();
        assertNotNull(indexMappings);
        MappingMetaData typeMappings = indexMappings.get(sharedResultsIndex);
        assertNotNull("expected " + sharedResultsIndex + " in " + indexMappings, typeMappings);
        Map<String, Object> mappings = typeMappings.getSourceAsMap();
        assertNotNull(mappings);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertNotNull("expected 'properties' field in " + mappings, properties);
        for (int i = 1; i <= numJobs; ++i) {
            String fieldName = "field" + i;
            assertNotNull("expected '" + fieldName + "' field in " + properties, properties.get(fieldName));
        }
    }

    public void testGetCalandarByJobId() throws Exception {
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar("empty calendar", Collections.emptyList(), null));
        calendars.add(new Calendar("foo calendar", Collections.singletonList("foo"), null));
        calendars.add(new Calendar("foo bar calendar", Arrays.asList("foo", "bar"), null));
        calendars.add(new Calendar("cat calendar",  Collections.singletonList("cat"), null));
        calendars.add(new Calendar("cat foo calendar", Arrays.asList("cat", "foo"), null));
        indexCalendars(calendars);

        List<Calendar> queryResult = getCalendars("ted");
        assertThat(queryResult, is(empty()));

        queryResult = getCalendars("foo");
        assertThat(queryResult, hasSize(3));
        Long matchedCount = queryResult.stream().filter(
                c -> c.getId().equals("foo calendar") || c.getId().equals("foo bar calendar") || c.getId().equals("cat foo calendar"))
                .count();
        assertEquals(Long.valueOf(3), matchedCount);

        queryResult = getCalendars("bar");
        assertThat(queryResult, hasSize(1));
        assertEquals("foo bar calendar", queryResult.get(0).getId());
    }

    public void testUpdateCalendar() throws Exception {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(createJob("foo").build(), false);
        mlBuilder.putJob(createJob("bar").build(), false);

        String calendarId = "empty calendar";
        Calendar emptyCal = new Calendar(calendarId, Collections.emptyList(), null);
        indexCalendars(Collections.singletonList(emptyCal));

        Set<String> addedIds = new HashSet<>();
        addedIds.add("foo");
        addedIds.add("bar");
        updateCalendar(calendarId, addedIds, Collections.emptySet(), mlBuilder.build());

        Calendar updated = getCalendar(calendarId);
        assertEquals(calendarId, updated.getId());
        assertEquals(addedIds, new HashSet<>(updated.getJobIds()));

        updateCalendar(calendarId, Collections.emptySet(), Collections.singleton("foo"), mlBuilder.build());

        updated = getCalendar(calendarId);
        assertEquals(calendarId, updated.getId());
        assertEquals(1, updated.getJobIds().size());
        assertEquals("bar", updated.getJobIds().get(0));
    }

    public void testRemoveJobFromCalendar() throws Exception {
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar("empty calendar", Collections.emptyList(), null));
        calendars.add(new Calendar("foo calendar", Collections.singletonList("foo"), null));
        calendars.add(new Calendar("foo bar calendar", Arrays.asList("foo", "bar"), null));
        calendars.add(new Calendar("cat calendar",  Collections.singletonList("cat"), null));
        calendars.add(new Calendar("cat foo calendar", Arrays.asList("cat", "foo"), null));
        indexCalendars(calendars);

        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        jobProvider.removeJobFromCalendars("bar", ActionListener.wrap(
                r -> latch.countDown(),
                e -> {
                    exceptionHolder.set(e);
                    latch.countDown();
                }));

        latch.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        List<Calendar> updatedCalendars = getCalendars(null);
        assertEquals(5, updatedCalendars.size());
        for (Calendar cal: updatedCalendars) {
            assertThat("bar", not(isIn(cal.getJobIds())));
        }

        Calendar catFoo = getCalendar("cat foo calendar");
        assertThat(catFoo.getJobIds(), contains("cat", "foo"));

        CountDownLatch latch2 = new CountDownLatch(1);
        jobProvider.removeJobFromCalendars("cat", ActionListener.wrap(
                r -> latch2.countDown(),
                e -> {
                    exceptionHolder.set(e);
                    latch2.countDown();
                }));

        latch2.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        updatedCalendars = getCalendars(null);
        assertEquals(5, updatedCalendars.size());
        for (Calendar cal: updatedCalendars) {
            assertThat("bar", not(isIn(cal.getJobIds())));
            assertThat("cat", not(isIn(cal.getJobIds())));
        }
    }

    private List<Calendar> getCalendars(String jobId) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<QueryPage<Calendar>> result = new AtomicReference<>();

        CalendarQueryBuilder query = new CalendarQueryBuilder();

        if (jobId != null) {
            query.jobId(jobId);
        }
        jobProvider.calendars(query, ActionListener.wrap(
                r -> {
                    result.set(r);
                    latch.countDown();
                },
                e -> {
                    exceptionHolder.set(e);
                    latch.countDown();
                }));

        latch.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        return result.get().results();
    }

    private void updateCalendar(String calendarId, Set<String> idsToAdd, Set<String> idsToRemove, MlMetadata mlMetadata)
            throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        jobProvider.updateCalendar(calendarId, idsToAdd, idsToRemove,
                r -> latch.countDown(),
                e -> {
                    exceptionHolder.set(e);
                    latch.countDown();
                });

        latch.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        client().admin().indices().prepareRefresh(MlMetaIndex.INDEX_NAME).get();
    }

    private Calendar getCalendar(String calendarId) throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Calendar> calendarHolder = new AtomicReference<>();
        jobProvider.calendar(calendarId, ActionListener.wrap(
                    c -> {
                        calendarHolder.set(c);
                        latch.countDown();
                        },
                    e -> {
                        exceptionHolder.set(e);
                        latch.countDown();
                    })
                );

        latch.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        return  calendarHolder.get();
    }

    public void testScheduledEvents() throws Exception {
        Job.Builder jobA = createJob("job_a");
        Job.Builder jobB = createJob("job_b");
        Job.Builder jobC = createJob("job_c");

        String calendarAId = "maintenance_a";
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar(calendarAId, Collections.singletonList("job_a"), null));

        ZonedDateTime now = ZonedDateTime.now();
        List<ScheduledEvent> events = new ArrayList<>();
        events.add(buildScheduledEvent("downtime", now.plusDays(1), now.plusDays(2), calendarAId));
        events.add(buildScheduledEvent("downtime_AA", now.plusDays(8), now.plusDays(9), calendarAId));
        events.add(buildScheduledEvent("downtime_AAA", now.plusDays(15), now.plusDays(16), calendarAId));

        String calendarABId = "maintenance_a_and_b";
        calendars.add(new Calendar(calendarABId, Arrays.asList("job_a", "job_b"), null));

        events.add(buildScheduledEvent("downtime_AB", now.plusDays(12), now.plusDays(13), calendarABId));

        indexCalendars(calendars);
        indexScheduledEvents(events);

        ScheduledEventsQueryBuilder query = new ScheduledEventsQueryBuilder();
        List<ScheduledEvent> returnedEvents = getScheduledEventsForJob(jobA.getId(), Collections.emptyList(), query);
        assertEquals(4, returnedEvents.size());
        assertEquals(events.get(0), returnedEvents.get(0));
        assertEquals(events.get(1), returnedEvents.get(1));
        assertEquals(events.get(3), returnedEvents.get(2));
        assertEquals(events.get(2), returnedEvents.get(3));

        returnedEvents = getScheduledEventsForJob(jobB.getId(), Collections.singletonList("unrelated-job-group"), query);
        assertEquals(1, returnedEvents.size());
        assertEquals(events.get(3), returnedEvents.get(0));

        returnedEvents = getScheduledEventsForJob(jobC.getId(), Collections.emptyList(), query);
        assertEquals(0, returnedEvents.size());

        // Test time filters
        // Lands halfway through the second event which should be returned
        query.start(Long.toString(now.plusDays(8).plusHours(1).toInstant().toEpochMilli()));
        // Lands halfway through the 3rd event which should be returned
        query.end(Long.toString(now.plusDays(12).plusHours(1).toInstant().toEpochMilli()));
        returnedEvents = getScheduledEventsForJob(jobA.getId(), Collections.emptyList(), query);
        assertEquals(2, returnedEvents.size());
        assertEquals(events.get(1), returnedEvents.get(0));
        assertEquals(events.get(3), returnedEvents.get(1));
    }

    public void testScheduledEventsForJob_withGroup() throws Exception {
        String groupA = "group-a";
        String groupB = "group-b";
        createJob("job-in-group-a", Collections.emptyList(), Collections.singletonList(groupA));
        createJob("job-in-group-a-and-b", Collections.emptyList(), Arrays.asList(groupA, groupB));

        String calendarAId = "calendar_a";
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar(calendarAId, Collections.singletonList(groupA), null));

        ZonedDateTime now = ZonedDateTime.now();
        List<ScheduledEvent> events = new ArrayList<>();
        events.add(buildScheduledEvent("downtime_A", now.plusDays(1), now.plusDays(2), calendarAId));

        String calendarBId = "calendar_b";
        calendars.add(new Calendar(calendarBId, Collections.singletonList(groupB), null));
        events.add(buildScheduledEvent("downtime_B", now.plusDays(12), now.plusDays(13), calendarBId));

        indexCalendars(calendars);
        indexScheduledEvents(events);

        ScheduledEventsQueryBuilder query = new ScheduledEventsQueryBuilder();
        List<ScheduledEvent> returnedEvents = getScheduledEventsForJob("job-in-group-a", Collections.singletonList(groupA), query);
        assertEquals(1, returnedEvents.size());
        assertEquals(events.get(0), returnedEvents.get(0));

        query = new ScheduledEventsQueryBuilder();
        returnedEvents = getScheduledEventsForJob("job-in-group-a-and-b", Collections.singletonList(groupB), query);
        assertEquals(1, returnedEvents.size());
        assertEquals(events.get(1), returnedEvents.get(0));
    }

    private ScheduledEvent buildScheduledEvent(String description, ZonedDateTime start, ZonedDateTime end, String calendarId) {
        return new ScheduledEvent.Builder()
            .description(description)
            .startTime(start.toInstant())
            .endTime(end.toInstant())
            .calendarId(calendarId)
            .build();
    }

    public void testGetAutodetectParams() throws Exception {
        String jobId = "test_get_autodetect_params";
        Job.Builder job = createJob(jobId, Arrays.asList("fruit", "tea"));

        String calendarId = "downtime";
        Calendar calendar = new Calendar(calendarId, Collections.singletonList(jobId), null);
        indexCalendars(Collections.singletonList(calendar));

        // index the param docs
        ZonedDateTime now = ZonedDateTime.now();
        List<ScheduledEvent> events = new ArrayList<>();
        // events in the past should be filtered out
        events.add(buildScheduledEvent("In the past", now.minusDays(7), now.minusDays(6), calendarId));
        events.add(buildScheduledEvent("A_downtime", now.plusDays(1), now.plusDays(2), calendarId));
        events.add(buildScheduledEvent("A_downtime2", now.plusDays(8), now.plusDays(9), calendarId));
        indexScheduledEvents(events);

        List<MlFilter> filters = new ArrayList<>();
        filters.add(MlFilter.builder("fruit").setItems("apple", "pear").build());
        filters.add(MlFilter.builder("tea").setItems("green", "builders").build());
        indexFilters(filters);

        DataCounts earliestCounts = DataCountsTests.createTestInstance(jobId);
        earliestCounts.setLatestRecordTimeStamp(new Date(1500000000000L));
        indexDataCounts(earliestCounts, jobId);
        DataCounts latestCounts = DataCountsTests.createTestInstance(jobId);
        latestCounts.setLatestRecordTimeStamp(new Date(1510000000000L));
        indexDataCounts(latestCounts, jobId);

        ModelSizeStats earliestSizeStats = new ModelSizeStats.Builder(jobId).setLogTime(new Date(1500000000000L)).build();
        ModelSizeStats latestSizeStats = new ModelSizeStats.Builder(jobId).setLogTime(new Date(1510000000000L)).build();
        indexModelSizeStats(earliestSizeStats);
        indexModelSizeStats(latestSizeStats);

        job.setModelSnapshotId("snap_1");
        ModelSnapshot snapshot = new ModelSnapshot.Builder(jobId).setSnapshotId("snap_1").build();
        indexModelSnapshot(snapshot);

        Quantiles quantiles = new Quantiles(jobId, new Date(), "quantile-state");
        indexQuantiles(quantiles);

        client().admin().indices().prepareRefresh(MlMetaIndex.INDEX_NAME, AnomalyDetectorsIndex.jobStateIndexPattern(),
                AnomalyDetectorsIndex.jobResultsAliasedName(jobId)).get();


        AutodetectParams params = getAutodetectParams(job.build(new Date()));

        // events
        assertNotNull(params.scheduledEvents());
        assertEquals(3, params.scheduledEvents().size());
        assertEquals(events.get(0), params.scheduledEvents().get(0));
        assertEquals(events.get(1), params.scheduledEvents().get(1));
        assertEquals(events.get(2), params.scheduledEvents().get(2));

        // filters
        assertNotNull(params.filters());
        assertEquals(2, params.filters().size());
        assertTrue(params.filters().contains(filters.get(0)));
        assertTrue(params.filters().contains(filters.get(1)));

        // datacounts
        assertNotNull(params.dataCounts());
        assertEquals(latestCounts, params.dataCounts());

        // model size stats
        assertNotNull(params.modelSizeStats());
        assertEquals(latestSizeStats, params.modelSizeStats());

        // model snapshot
        assertNotNull(params.modelSnapshot());
        assertEquals(snapshot, params.modelSnapshot());

        // quantiles
        assertNotNull(params.quantiles());
        assertEquals(quantiles, params.quantiles());
    }

    private AutodetectParams getAutodetectParams(Job job) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<AutodetectParams> searchResultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobProvider.getAutodetectParams(job, params -> {
            searchResultHolder.set(params);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        });

        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }

        return searchResultHolder.get();
    }

    private List<ScheduledEvent> getScheduledEventsForJob(String jobId, List<String> jobGroups, ScheduledEventsQueryBuilder query)
            throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<ScheduledEvent>> searchResultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobProvider.scheduledEventsForJob(jobId, jobGroups, query, ActionListener.wrap(
                params -> {
            searchResultHolder.set(params);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        }));

        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }

        return searchResultHolder.get().results();
    }

    private Job.Builder createJob(String jobId) {
        return createJob(jobId, Collections.emptyList());
    }

    private Job.Builder createJob(String jobId, List<String> filterIds) {
        return createJob(jobId, filterIds, Collections.emptyList());
    }

    private Job.Builder createJob(String jobId, List<String> filterIds, List<String> jobGroups) {
        Job.Builder builder = new Job.Builder(jobId);
        builder.setGroups(jobGroups);
        AnalysisConfig.Builder ac = createAnalysisConfig("by_field", filterIds);
        DataDescription.Builder dc = new DataDescription.Builder();
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(dc);

        PutJobAction.Request request = new PutJobAction.Request(builder);
        client().execute(PutJobAction.INSTANCE, request).actionGet();
        return builder;
    }

    private AnalysisConfig.Builder createAnalysisConfig(String byFieldName, List<String> filterIds) {
        Detector.Builder detector = new Detector.Builder("mean", "field");
        detector.setByFieldName(byFieldName);
        List<DetectionRule> rules = new ArrayList<>();

        for (String filterId : filterIds) {
            RuleScope.Builder ruleScope = RuleScope.builder();
            ruleScope.include(byFieldName, filterId);

            rules.add(new DetectionRule.Builder(ruleScope).setActions(RuleAction.SKIP_RESULT).build());
        }
        detector.setRules(rules);

        return new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
    }

    private void indexScheduledEvents(List<ScheduledEvent> events) throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (ScheduledEvent event : events) {
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME);
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(
                    ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
                indexRequest.source(event.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        BulkResponse response = bulkRequest.execute().actionGet();
        if (response.hasFailures()) {
            throw new IllegalStateException(Strings.toString(response));
        }
    }

    private void indexDataCounts(DataCounts counts, String jobId) {
        JobDataCountsPersister persister = new JobDataCountsPersister(client(), resultsPersisterService, auditor);
        persister.persistDataCounts(jobId, counts);
    }

    private void indexFilters(List<MlFilter> filters) throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (MlFilter filter : filters) {
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME).id(filter.documentId());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(
                    ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
                indexRequest.source(filter.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        bulkRequest.execute().actionGet();
    }

    private void indexModelSizeStats(ModelSizeStats modelSizeStats) {
        JobResultsPersister persister = new JobResultsPersister(client(), resultsPersisterService, auditor);
        persister.persistModelSizeStats(modelSizeStats, () -> true);
    }

    private void indexModelSnapshot(ModelSnapshot snapshot) {
        JobResultsPersister persister = new JobResultsPersister(client(), resultsPersisterService, auditor);
        persister.persistModelSnapshot(snapshot, WriteRequest.RefreshPolicy.IMMEDIATE, () -> true);
    }

    private void indexQuantiles(Quantiles quantiles) {
        JobResultsPersister persister = new JobResultsPersister(client(), resultsPersisterService, auditor);
        persister.persistQuantiles(quantiles, () -> true);
    }

    private void indexCalendars(List<Calendar> calendars) throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (Calendar calendar: calendars) {
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME).id(calendar.documentId());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(
                    Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
                indexRequest.source(calendar.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        bulkRequest.execute().actionGet();
    }
}
