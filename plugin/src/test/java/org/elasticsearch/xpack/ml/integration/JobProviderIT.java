/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.XPackSingleNodeTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.calendars.SpecialEvent;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.Connective;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.job.config.RuleAction;
import org.elasticsearch.xpack.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.CalendarQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.SpecialEventsQueryBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCountsTests;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;


public class JobProviderIT extends XPackSingleNodeTestCase {

    private JobProvider jobProvider;

    @Override
    protected Settings nodeSettings()  {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        return newSettings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(XPackPlugin.class);
    }

    @Before
    public void createComponents() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        jobProvider = new JobProvider(client(), builder.build());
        waitForMlTemplates();
    }

    private void waitForMlTemplates() throws Exception {
        // block until the templates are installed
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue("Timed out waiting for the ML templates to be installed",
                    MachineLearning.allTemplatesInstalled(state));
        });
    }

    public void testGetCalandarByJobId() throws Exception {
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar("empty calendar", Collections.emptyList()));
        calendars.add(new Calendar("foo calendar", Collections.singletonList("foo")));
        calendars.add(new Calendar("foo bar calendar", Arrays.asList("foo", "bar")));
        calendars.add(new Calendar("cat calendar",  Collections.singletonList("cat")));
        calendars.add(new Calendar("cat foo calendar", Arrays.asList("cat", "foo")));
        indexCalendars(calendars);

        List<Calendar> queryResult = getCalendars("ted");
        assertThat(queryResult, is(empty()));

        queryResult = getCalendars("foo");
        assertThat(queryResult, hasSize(3));
        Long matchedCount = queryResult.stream().filter(
                c -> c.getId().equals("foo calendar") || c.getId().equals("foo bar calendar") || c.getId().equals("cat foo calendar"))
                .collect(Collectors.counting());
        assertEquals(new Long(3), matchedCount);

        queryResult = getCalendars("bar");
        assertThat(queryResult, hasSize(1));
        assertEquals("foo bar calendar", queryResult.get(0).getId());
    }

    public void testUpdateCalendar() throws Exception {
        String calendarId = "empty calendar";
        Calendar emptyCal = new Calendar(calendarId, Collections.emptyList());
        indexCalendars(Collections.singletonList(emptyCal));

        Set<String> addedIds = new HashSet<>();
        addedIds.add("foo");
        addedIds.add("bar");
        updateCalendar(calendarId, addedIds, Collections.emptySet());

        Calendar updated = getCalendar(calendarId);
        assertEquals(calendarId, updated.getId());
        assertEquals(addedIds, new HashSet<>(updated.getJobIds()));

        Set<String> removedIds = new HashSet<>();
        removedIds.add("foo");
        updateCalendar(calendarId, Collections.emptySet(), removedIds);

        updated = getCalendar(calendarId);
        assertEquals(calendarId, updated.getId());
        assertEquals(1, updated.getJobIds().size());
        assertEquals("bar", updated.getJobIds().get(0));
    }

    public void testRemoveJobFromCalendar() throws Exception {
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar("empty calendar", Collections.emptyList()));
        calendars.add(new Calendar("foo calendar", Collections.singletonList("foo")));
        calendars.add(new Calendar("foo bar calendar", Arrays.asList("foo", "bar")));
        calendars.add(new Calendar("cat calendar",  Collections.singletonList("cat")));
        calendars.add(new Calendar("cat foo calendar", Arrays.asList("cat", "foo")));
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

    private void updateCalendar(String calendarId, Set<String> idsToAdd, Set<String> idsToRemove) throws Exception {
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

    public void testSpecialEvents() throws Exception {
        Job.Builder jobA = createJob("job_a");
        Job.Builder jobB = createJob("job_b");
        Job.Builder jobC = createJob("job_c");

        String calendarAId = "maintenance_a";
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar(calendarAId, Collections.singletonList("job_a")));

        ZonedDateTime now = ZonedDateTime.now();
        List<SpecialEvent> events = new ArrayList<>();
        events.add(buildSpecialEvent("downtime", now.plusDays(1), now.plusDays(2), calendarAId));
        events.add(buildSpecialEvent("downtime_AA", now.plusDays(8), now.plusDays(9), calendarAId));
        events.add(buildSpecialEvent("downtime_AAA", now.plusDays(15), now.plusDays(16), calendarAId));

        String calendarABId = "maintenance_a_and_b";
        calendars.add(new Calendar(calendarABId, Arrays.asList("job_a", "job_b")));

        events.add(buildSpecialEvent("downtime_AB", now.plusDays(12), now.plusDays(13), calendarABId));

        indexCalendars(calendars);
        indexSpecialEvents(events);

        SpecialEventsQueryBuilder query = new SpecialEventsQueryBuilder();
        List<SpecialEvent> returnedEvents = getSpecialEventsForJob(jobA.getId(), query);
        assertEquals(4, returnedEvents.size());
        assertEquals(events.get(0), returnedEvents.get(0));
        assertEquals(events.get(1), returnedEvents.get(1));
        assertEquals(events.get(3), returnedEvents.get(2));
        assertEquals(events.get(2), returnedEvents.get(3));

        returnedEvents = getSpecialEventsForJob(jobB.getId(), query);
        assertEquals(1, returnedEvents.size());
        assertEquals(events.get(3), returnedEvents.get(0));

        returnedEvents = getSpecialEventsForJob(jobC.getId(), query);
        assertEquals(0, returnedEvents.size());

        // Test time filters
        // Lands halfway through the second event which should be returned
        query.after(Long.toString(now.plusDays(8).plusHours(1).toInstant().toEpochMilli()));
        // Lands halfway through the 3rd event which should be returned
        query.before(Long.toString(now.plusDays(12).plusHours(1).toInstant().toEpochMilli()));
        returnedEvents = getSpecialEventsForJob(jobA.getId(), query);
        assertEquals(2, returnedEvents.size());
        assertEquals(events.get(1), returnedEvents.get(0));
        assertEquals(events.get(3), returnedEvents.get(1));
    }

    private SpecialEvent buildSpecialEvent(String description, ZonedDateTime start, ZonedDateTime end, String calendarId) {
        return new SpecialEvent.Builder().description(description).startTime(start).endTime(end).calendarId(calendarId).build();
    }

    public void testGetAutodetectParams() throws Exception {
        String jobId = "test_get_autodetect_params";
        Job.Builder job = createJob(jobId, Arrays.asList("fruit", "tea"));

        String calendarId = "downtime";
        Calendar calendar = new Calendar(calendarId, Collections.singletonList(jobId));
        indexCalendars(Collections.singletonList(calendar));

        // index the param docs
        ZonedDateTime now = ZonedDateTime.now();
        List<SpecialEvent> events = new ArrayList<>();
        // events in the past should be filtered out
        events.add(buildSpecialEvent("In the past", now.minusDays(7), now.minusDays(6), calendarId));
        events.add(buildSpecialEvent("A_downtime", now.plusDays(1), now.plusDays(2), calendarId));
        events.add(buildSpecialEvent("A_downtime2", now.plusDays(8), now.plusDays(9), calendarId));
        indexSpecialEvents(events);

        List<MlFilter> filters = new ArrayList<>();
        filters.add(new MlFilter("fruit", Arrays.asList("apple", "pear")));
        filters.add(new MlFilter("tea", Arrays.asList("green", "builders")));
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

        client().admin().indices().prepareRefresh(MlMetaIndex.INDEX_NAME, AnomalyDetectorsIndex.jobStateIndexName(),
                AnomalyDetectorsIndex.jobResultsAliasedName(jobId)).get();


        AutodetectParams params = getAutodetectParams(job.build(new Date()));

        // special events
        assertNotNull(params.specialEvents());
        assertEquals(3, params.specialEvents().size());
        assertEquals(events.get(0), params.specialEvents().get(0));
        assertEquals(events.get(1), params.specialEvents().get(1));
        assertEquals(events.get(2), params.specialEvents().get(2));

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

    private List<SpecialEvent> getSpecialEventsForJob(String jobId, SpecialEventsQueryBuilder query) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<SpecialEvent>> searchResultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobProvider.specialEventsForJob(jobId, query, ActionListener.wrap(
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
        Job.Builder builder = new Job.Builder(jobId);
        AnalysisConfig.Builder ac = createAnalysisConfig(filterIds);
        DataDescription.Builder dc = new DataDescription.Builder();
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(dc);

        PutJobAction.Request request = new PutJobAction.Request(builder);
        PutJobAction.Response response = client().execute(PutJobAction.INSTANCE, request).actionGet();
        assertTrue(response.isAcknowledged());
        return builder;
    }

    private AnalysisConfig.Builder createAnalysisConfig(List<String> filterIds) {
        Detector.Builder detector = new Detector.Builder("mean", "field");
        detector.setByFieldName("by_field");

        if (!filterIds.isEmpty()) {
            List<RuleCondition> conditions = new ArrayList<>();

            for (String filterId : filterIds) {
                conditions.add(RuleCondition.createCategorical("by_field", filterId));
            }

            DetectionRule.Builder rule = new DetectionRule.Builder(conditions)
                    .setActions(RuleAction.FILTER_RESULTS)
                    .setConditionsConnective(Connective.OR);

            detector.setRules(Collections.singletonList(rule.build()));
        }

        return new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
    }

    private void indexSpecialEvents(List<SpecialEvent> events) throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (SpecialEvent event : events) {
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE);
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(MlMetaIndex.INCLUDE_TYPE_KEY, "true"));
                indexRequest.source(event.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        BulkResponse response = bulkRequest.execute().actionGet();
        if (response.hasFailures()) {
            throw new IllegalStateException(Strings.toString(response));
        }
    }

    private void indexDataCounts(DataCounts counts, String jobId) throws Exception {
        JobDataCountsPersister persister = new JobDataCountsPersister(nodeSettings(), client());

        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        persister.persistDataCounts(jobId, counts, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                assertTrue(aBoolean);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorHolder.set(e);
                latch.countDown();
            }
        });

        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }
    }

    private void indexFilters(List<MlFilter> filters) throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (MlFilter filter : filters) {
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE, filter.documentId());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(MlMetaIndex.INCLUDE_TYPE_KEY, "true"));
                indexRequest.source(filter.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        bulkRequest.execute().actionGet();
    }

    private void indexModelSizeStats(ModelSizeStats modelSizeStats) {
        JobResultsPersister persister = new JobResultsPersister(nodeSettings(), client());
        persister.persistModelSizeStats(modelSizeStats);
    }

    private void indexModelSnapshot(ModelSnapshot snapshot) {
        JobResultsPersister persister = new JobResultsPersister(nodeSettings(), client());
        persister.persistModelSnapshot(snapshot, WriteRequest.RefreshPolicy.IMMEDIATE);
    }

    private void indexQuantiles(Quantiles quantiles) {
        JobResultsPersister persister = new JobResultsPersister(nodeSettings(), client());
        persister.persistQuantiles(quantiles);
    }

    private void indexCalendars(List<Calendar> calendars) throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (Calendar calendar: calendars) {
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE, calendar.documentId());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(MlMetaIndex.INCLUDE_TYPE_KEY, "true"));
                indexRequest.source(calendar.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        bulkRequest.execute().actionGet();
    }

    private ZonedDateTime createZonedDateTime(long epochMs) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
    }
}
