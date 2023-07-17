/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
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
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
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
import java.time.Instant;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;

public class JobResultsProviderIT extends MlSingleNodeTestCase {

    private JobResultsProvider jobProvider;
    private ResultsPersisterService resultsPersisterService;
    private JobResultsPersister jobResultsPersister;
    private AnomalyDetectionAuditor auditor;

    @Before
    public void createComponents() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        jobProvider = new JobResultsProvider(client(), builder.build(), TestIndexNameExpressionResolver.newInstance());
        ThreadPool tp = mockThreadPool();
        ClusterSettings clusterSettings = new ClusterSettings(
            builder.build(),
            new HashSet<>(
                Arrays.asList(
                    InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                    ClusterService.USER_DEFINED_METADATA,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING
                )
            )
        );
        ClusterService clusterService = new ClusterService(builder.build(), clusterSettings, tp, null);

        OriginSettingClient originSettingClient = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        resultsPersisterService = new ResultsPersisterService(tp, originSettingClient, clusterService, builder.build());
        jobResultsPersister = new JobResultsPersister(originSettingClient, resultsPersisterService);
        // We can't change the signature of createComponents to e.g. pass differing values of includeNodeInfo to pass to the
        // AnomalyDetectionAuditor constructor. Instead we generate a random boolean value for that purpose.
        boolean includeNodeInfo = randomBoolean();
        auditor = new AnomalyDetectionAuditor(client(), clusterService, includeNodeInfo);
        waitForMlTemplates();
    }

    public void testPutJob_CreatesResultsIndex() {

        Job.Builder job1 = new Job.Builder("first_job");
        job1.setAnalysisConfig(createAnalysisConfig("by_field_1", Collections.emptyList()));
        job1.setDataDescription(new DataDescription.Builder());

        // Put fist job. This should create the results index as it's the first job.
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job1)).actionGet();

        String sharedResultsIndex = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
        Map<String, Object> mappingProperties = getIndexMappingProperties(sharedResultsIndex);

        // Assert mappings have a few fields from the template
        assertThat(mappingProperties.keySet(), hasItems("anomaly_score", "bucket_count"));
        // Assert mappings have the by field
        assertThat(mappingProperties.keySet(), hasItem("by_field_1"));

        // Check aliases have been created
        assertThat(
            getAliases(sharedResultsIndex),
            containsInAnyOrder(
                AnomalyDetectorsIndex.jobResultsAliasedName(job1.getId()),
                AnomalyDetectorsIndex.resultsWriteAlias(job1.getId())
            )
        );

        // Now let's create a second job to test things work when the index exists already
        assertThat(mappingProperties.keySet(), not(hasItem("by_field_2")));

        Job.Builder job2 = new Job.Builder("second_job");
        job2.setAnalysisConfig(createAnalysisConfig("by_field_2", Collections.emptyList()));
        job2.setDataDescription(new DataDescription.Builder());

        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job2)).actionGet();

        mappingProperties = getIndexMappingProperties(sharedResultsIndex);

        // Assert mappings have a few fields from the template
        assertThat(mappingProperties.keySet(), hasItems("anomaly_score", "bucket_count"));
        // Assert mappings have the by field
        assertThat(mappingProperties.keySet(), hasItems("by_field_1", "by_field_2"));

        // Check aliases have been created
        assertThat(
            getAliases(sharedResultsIndex),
            containsInAnyOrder(
                AnomalyDetectorsIndex.jobResultsAliasedName(job1.getId()),
                AnomalyDetectorsIndex.resultsWriteAlias(job1.getId()),
                AnomalyDetectorsIndex.jobResultsAliasedName(job2.getId()),
                AnomalyDetectorsIndex.resultsWriteAlias(job2.getId())
            )
        );
    }

    public void testPutJob_WithCustomResultsIndex() {
        Job.Builder job = new Job.Builder("foo");
        job.setResultsIndexName("bar");
        job.setAnalysisConfig(createAnalysisConfig("by_field", Collections.emptyList()));
        job.setDataDescription(new DataDescription.Builder());

        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        String customIndex = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-bar";
        Map<String, Object> mappingProperties = getIndexMappingProperties(customIndex);

        // Assert mappings have a few fields from the template
        assertThat(mappingProperties.keySet(), hasItems("anomaly_score", "bucket_count"));
        // Assert mappings have the by field
        assertThat(mappingProperties.keySet(), hasItem("by_field"));

        // Check aliases have been created
        assertThat(
            getAliases(customIndex),
            containsInAnyOrder(
                AnomalyDetectorsIndex.jobResultsAliasedName(job.getId()),
                AnomalyDetectorsIndex.resultsWriteAlias(job.getId())
            )
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40134")
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
        Map<String, MappingMetadata> indexMappings = response.getMappings();
        assertNotNull(indexMappings);
        MappingMetadata typeMappings = indexMappings.get(sharedResultsIndex);
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
        calendars.add(new Calendar("cat calendar", Collections.singletonList("cat"), null));
        calendars.add(new Calendar("cat foo calendar", Arrays.asList("cat", "foo"), null));
        indexCalendars(calendars);

        List<Calendar> queryResult = getCalendars(CalendarQueryBuilder.builder().jobId("ted"));
        assertThat(queryResult, is(empty()));

        queryResult = getCalendars(CalendarQueryBuilder.builder().jobId("foo"));
        assertThat(queryResult, hasSize(3));
        Long matchedCount = queryResult.stream()
            .filter(c -> c.getId().equals("foo calendar") || c.getId().equals("foo bar calendar") || c.getId().equals("cat foo calendar"))
            .count();
        assertEquals(Long.valueOf(3), matchedCount);

        queryResult = getCalendars(CalendarQueryBuilder.builder().jobId("bar"));
        assertThat(queryResult, hasSize(1));
        assertEquals("foo bar calendar", queryResult.get(0).getId());
    }

    public void testGetCalandarById() throws Exception {
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar("empty calendar", Collections.emptyList(), null));
        calendars.add(new Calendar("foo calendar", Collections.singletonList("foo"), null));
        calendars.add(new Calendar("foo bar calendar", Arrays.asList("foo", "bar"), null));
        calendars.add(new Calendar("cat calendar", Collections.singletonList("cat"), null));
        calendars.add(new Calendar("cat foo calendar", Arrays.asList("cat", "foo"), null));
        indexCalendars(calendars);

        List<Calendar> queryResult = getCalendars(CalendarQueryBuilder.builder().calendarIdTokens(new String[] { "foo*" }).sort(true));
        assertThat(queryResult, hasSize(2));
        assertThat(queryResult.get(0).getId(), equalTo("foo bar calendar"));
        assertThat(queryResult.get(1).getId(), equalTo("foo calendar"));

        queryResult = getCalendars(
            CalendarQueryBuilder.builder().calendarIdTokens(new String[] { "foo calendar", "cat calendar" }).sort(true)
        );
        assertThat(queryResult, hasSize(2));
        assertThat(queryResult.get(0).getId(), equalTo("cat calendar"));
        assertThat(queryResult.get(1).getId(), equalTo("foo calendar"));
    }

    public void testGetCalendarByIdAndPaging() throws Exception {
        List<Calendar> calendars = new ArrayList<>();
        calendars.add(new Calendar("empty calendar", Collections.emptyList(), null));
        calendars.add(new Calendar("foo calendar", Collections.singletonList("foo"), null));
        calendars.add(new Calendar("foo bar calendar", Arrays.asList("foo", "bar"), null));
        calendars.add(new Calendar("cat calendar", Collections.singletonList("cat"), null));
        calendars.add(new Calendar("cat foo calendar", Arrays.asList("cat", "foo"), null));
        indexCalendars(calendars);

        List<Calendar> queryResult = getCalendars(
            CalendarQueryBuilder.builder().calendarIdTokens(new String[] { "foo*" }).pageParams(new PageParams(0, 1)).sort(true)
        );
        assertThat(queryResult, hasSize(1));
        assertThat(queryResult.get(0).getId(), equalTo("foo bar calendar"));

        queryResult = getCalendars(
            CalendarQueryBuilder.builder()
                .calendarIdTokens(new String[] { "foo calendar", "cat calendar" })
                .sort(true)
                .pageParams(new PageParams(1, 1))
        );
        assertThat(queryResult, hasSize(1));
        assertThat(queryResult.get(0).getId(), equalTo("foo calendar"));
    }

    public void testUpdateCalendar() throws Exception {

        String calendarId = "empty calendar";
        Calendar emptyCal = new Calendar(calendarId, Collections.emptyList(), null);
        indexCalendars(Collections.singletonList(emptyCal));

        Set<String> addedIds = new HashSet<>();
        addedIds.add("foo");
        addedIds.add("bar");
        updateCalendar(calendarId, addedIds, Collections.emptySet());

        Calendar updated = getCalendar(calendarId);
        assertEquals(calendarId, updated.getId());
        assertEquals(addedIds, new HashSet<>(updated.getJobIds()));

        updateCalendar(calendarId, Collections.emptySet(), Collections.singleton("foo"));

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
        calendars.add(new Calendar("cat calendar", Collections.singletonList("cat"), null));
        calendars.add(new Calendar("cat foo calendar", Arrays.asList("cat", "foo"), null));
        indexCalendars(calendars);

        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        jobProvider.removeJobFromCalendars("bar", ActionListener.wrap(r -> latch.countDown(), e -> {
            exceptionHolder.set(e);
            latch.countDown();
        }));

        latch.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        List<Calendar> updatedCalendars = getCalendars(CalendarQueryBuilder.builder());
        assertEquals(5, updatedCalendars.size());
        for (Calendar cal : updatedCalendars) {
            assertThat("bar", is(not(in(cal.getJobIds()))));
        }

        Calendar catFoo = getCalendar("cat foo calendar");
        assertThat(catFoo.getJobIds(), contains("cat", "foo"));

        CountDownLatch latch2 = new CountDownLatch(1);
        jobProvider.removeJobFromCalendars("cat", ActionListener.wrap(r -> latch2.countDown(), e -> {
            exceptionHolder.set(e);
            latch2.countDown();
        }));

        latch2.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        updatedCalendars = getCalendars(CalendarQueryBuilder.builder());
        assertEquals(5, updatedCalendars.size());
        for (Calendar cal : updatedCalendars) {
            assertThat("bar", is(not(in(cal.getJobIds()))));
            assertThat("cat", is(not(in(cal.getJobIds()))));
        }
    }

    public void testGetDataCountsModelSizeAndTimingStatsWithNoDocs() throws Exception {
        Job.Builder job = new Job.Builder("first_job");
        job.setAnalysisConfig(createAnalysisConfig("by_field_1", Collections.emptyList()));
        job.setDataDescription(new DataDescription.Builder());

        // Put first job. This should create the results index as it's the first job.
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();
        AtomicReference<DataCounts> dataCountsAtomicReference = new AtomicReference<>();
        AtomicReference<ModelSizeStats> modelSizeStatsAtomicReference = new AtomicReference<>();
        AtomicReference<TimingStats> timingStatsAtomicReference = new AtomicReference<>();
        AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();

        getDataCountsModelSizeAndTimingStats(
            job.getId(),
            dataCountsAtomicReference::set,
            modelSizeStatsAtomicReference::set,
            timingStatsAtomicReference::set,
            exceptionAtomicReference::set
        );

        if (exceptionAtomicReference.get() != null) {
            throw exceptionAtomicReference.get();
        }

        assertThat(dataCountsAtomicReference.get().getJobId(), equalTo(job.getId()));
        assertThat(modelSizeStatsAtomicReference.get().getJobId(), equalTo(job.getId()));
        assertThat(timingStatsAtomicReference.get().getJobId(), equalTo(job.getId()));
    }

    public void testGetDataCountsModelSizeAndTimingStatsWithSomeDocs() throws Exception {
        Job.Builder job = new Job.Builder("first_job");
        job.setAnalysisConfig(createAnalysisConfig("by_field_1", Collections.emptyList()));
        job.setDataDescription(new DataDescription.Builder());

        // Put first job. This should create the results index as it's the first job.
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();
        AtomicReference<DataCounts> dataCountsAtomicReference = new AtomicReference<>();
        AtomicReference<ModelSizeStats> modelSizeStatsAtomicReference = new AtomicReference<>();
        AtomicReference<TimingStats> timingStatsAtomicReference = new AtomicReference<>();
        AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();

        CheckedSupplier<Void, Exception> setOrThrow = () -> {
            getDataCountsModelSizeAndTimingStats(
                job.getId(),
                dataCountsAtomicReference::set,
                modelSizeStatsAtomicReference::set,
                timingStatsAtomicReference::set,
                exceptionAtomicReference::set
            );

            if (exceptionAtomicReference.get() != null) {
                throw exceptionAtomicReference.get();
            }
            return null;
        };

        ModelSizeStats storedModelSizeStats = new ModelSizeStats.Builder(job.getId()).setModelBytes(10L).build();
        jobResultsPersister.persistModelSizeStats(storedModelSizeStats, () -> false);
        jobResultsPersister.commitWrites(job.getId(), JobResultsPersister.CommitType.RESULTS);

        setOrThrow.get();
        assertThat(dataCountsAtomicReference.get().getJobId(), equalTo(job.getId()));
        assertThat(modelSizeStatsAtomicReference.get(), equalTo(storedModelSizeStats));
        assertThat(timingStatsAtomicReference.get().getJobId(), equalTo(job.getId()));

        TimingStats storedTimingStats = new TimingStats(job.getId());
        storedTimingStats.updateStats(10);

        jobResultsPersister.bulkPersisterBuilder(job.getId()).persistTimingStats(storedTimingStats).executeRequest();
        jobResultsPersister.commitWrites(job.getId(), JobResultsPersister.CommitType.RESULTS);

        setOrThrow.get();

        assertThat(dataCountsAtomicReference.get().getJobId(), equalTo(job.getId()));
        assertThat(modelSizeStatsAtomicReference.get(), equalTo(storedModelSizeStats));
        assertThat(timingStatsAtomicReference.get(), equalTo(storedTimingStats));

        DataCounts storedDataCounts = new DataCounts(job.getId());
        storedDataCounts.incrementInputBytes(1L);
        storedDataCounts.incrementMissingFieldCount(1L);
        JobDataCountsPersister jobDataCountsPersister = new JobDataCountsPersister(client(), resultsPersisterService, auditor);
        jobDataCountsPersister.persistDataCounts(job.getId(), storedDataCounts, true);
        jobResultsPersister.commitWrites(job.getId(), JobResultsPersister.CommitType.RESULTS);

        setOrThrow.get();
        assertThat(dataCountsAtomicReference.get(), equalTo(storedDataCounts));
        assertThat(modelSizeStatsAtomicReference.get(), equalTo(storedModelSizeStats));
        assertThat(timingStatsAtomicReference.get(), equalTo(storedTimingStats));
    }

    private Map<String, Object> getIndexMappingProperties(String index) {
        GetMappingsRequest request = new GetMappingsRequest().indices(index);
        GetMappingsResponse response = client().execute(GetMappingsAction.INSTANCE, request).actionGet();
        Map<String, MappingMetadata> indexMappings = response.getMappings();
        assertNotNull(indexMappings);
        MappingMetadata typeMappings = indexMappings.get(index);
        assertNotNull("expected " + index + " in " + indexMappings, typeMappings);
        Map<String, Object> mappings = typeMappings.getSourceAsMap();
        assertNotNull(mappings);

        // Assert _meta info is present
        assertThat(mappings.keySet(), hasItem("_meta"));
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) mappings.get("_meta");
        assertThat(meta.keySet(), hasItem("version"));
        assertThat(meta.get("version"), equalTo(MlConfigVersion.CURRENT.toString()));

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertNotNull("expected 'properties' field in " + mappings, properties);
        return properties;
    }

    private Set<String> getAliases(String index) {
        GetAliasesResponse getAliasesResponse = client().admin().indices().getAliases(new GetAliasesRequest().indices(index)).actionGet();
        Map<String, List<AliasMetadata>> aliases = getAliasesResponse.getAliases();
        assertThat(aliases.containsKey(index), is(true));
        List<AliasMetadata> aliasMetadataList = aliases.get(index);
        for (AliasMetadata aliasMetadata : aliasMetadataList) {
            assertThat("Anomalies aliases should be hidden but are not: " + aliases, aliasMetadata.isHidden(), is(true));
        }
        return aliasMetadataList.stream().map(AliasMetadata::alias).collect(Collectors.toSet());
    }

    private List<Calendar> getCalendars(CalendarQueryBuilder query) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<QueryPage<Calendar>> result = new AtomicReference<>();

        jobProvider.calendars(query, ActionListener.wrap(r -> {
            result.set(r);
            latch.countDown();
        }, e -> {
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
        jobProvider.updateCalendar(calendarId, idsToAdd, idsToRemove, r -> latch.countDown(), e -> {
            exceptionHolder.set(e);
            latch.countDown();
        });

        latch.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        client().admin().indices().prepareRefresh(MlMetaIndex.indexName()).get();
    }

    private Calendar getCalendar(String calendarId) throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Calendar> calendarHolder = new AtomicReference<>();
        jobProvider.calendar(calendarId, ActionListener.wrap(c -> {
            calendarHolder.set(c);
            latch.countDown();
        }, e -> {
            exceptionHolder.set(e);
            latch.countDown();
        }));

        latch.await();
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        return calendarHolder.get();
    }

    private void getDataCountsModelSizeAndTimingStats(
        String jobId,
        Consumer<DataCounts> dataCountsConsumer,
        Consumer<ModelSizeStats> modelSizeStatsConsumer,
        Consumer<TimingStats> timingStatsConsumer,
        Consumer<Exception> exceptionConsumer
    ) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        jobProvider.getDataCountsModelSizeAndTimingStats(jobId, null, (dataCounts, modelSizeStats, timingStats) -> {
            dataCountsConsumer.accept(dataCounts);
            modelSizeStatsConsumer.accept(modelSizeStats);
            timingStatsConsumer.accept(timingStats);
            latch.countDown();
        }, e -> {
            exceptionConsumer.accept(e);
            latch.countDown();
        });
        latch.await();
    }

    public void testScheduledEventsForJobs() throws Exception {
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

    public void testScheduledEvents() throws Exception {
        createJob("job_a");
        createJob("job_b");
        createJob("job_c");

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

        List<ScheduledEvent> returnedEvents = getScheduledEvents(new ScheduledEventsQueryBuilder());
        assertEquals(4, returnedEvents.size());
        assertEquals(events.get(0), returnedEvents.get(0));
        assertEquals(events.get(1), returnedEvents.get(1));
        assertEquals(events.get(3), returnedEvents.get(2));
        assertEquals(events.get(2), returnedEvents.get(3));

        returnedEvents = getScheduledEvents(ScheduledEventsQueryBuilder.builder().calendarIds(new String[] { "maintenance_a" }));
        assertEquals(3, returnedEvents.size());
        assertEquals(events.get(0), returnedEvents.get(0));
        assertEquals(events.get(1), returnedEvents.get(1));
        assertEquals(events.get(2), returnedEvents.get(2));

        returnedEvents = getScheduledEvents(
            ScheduledEventsQueryBuilder.builder().calendarIds(new String[] { "maintenance_a", "maintenance_a_and_b" })
        );
        assertEquals(4, returnedEvents.size());
        assertEquals(events.get(0), returnedEvents.get(0));
        assertEquals(events.get(1), returnedEvents.get(1));
        assertEquals(events.get(3), returnedEvents.get(2));
        assertEquals(events.get(2), returnedEvents.get(3));

        returnedEvents = getScheduledEvents(ScheduledEventsQueryBuilder.builder().calendarIds(new String[] { "maintenance_a*" }));
        assertEquals(4, returnedEvents.size());
        assertEquals(events.get(0), returnedEvents.get(0));
        assertEquals(events.get(1), returnedEvents.get(1));
        assertEquals(events.get(3), returnedEvents.get(2));
        assertEquals(events.get(2), returnedEvents.get(3));
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
        return new ScheduledEvent.Builder().description(description)
            .startTime(start.toInstant())
            .endTime(end.toInstant())
            .calendarId(calendarId)
            .build();
    }

    public void testGetSnapshots() {
        String jobId = "test_get_snapshots";
        createJob(jobId);
        indexModelSnapshot(
            new ModelSnapshot.Builder(jobId).setSnapshotId("snap_2")
                .setTimestamp(Date.from(Instant.ofEpochMilli(10)))
                .setMinVersion(MlConfigVersion.V_7_4_0)
                .setQuantiles(new Quantiles(jobId, Date.from(Instant.ofEpochMilli(10)), randomAlphaOfLength(20)))
                .build()
        );
        indexModelSnapshot(
            new ModelSnapshot.Builder(jobId).setSnapshotId("snap_1")
                .setTimestamp(Date.from(Instant.ofEpochMilli(11)))
                .setMinVersion(MlConfigVersion.V_7_2_0)
                .setQuantiles(new Quantiles(jobId, Date.from(Instant.ofEpochMilli(11)), randomAlphaOfLength(20)))
                .build()
        );
        indexModelSnapshot(
            new ModelSnapshot.Builder(jobId).setSnapshotId("other_snap")
                .setTimestamp(Date.from(Instant.ofEpochMilli(12)))
                .setMinVersion(MlConfigVersion.V_7_3_0)
                .setQuantiles(new Quantiles(jobId, Date.from(Instant.ofEpochMilli(12)), randomAlphaOfLength(20)))
                .build()
        );
        createJob("other_job");
        indexModelSnapshot(
            new ModelSnapshot.Builder("other_job").setSnapshotId("other_snap")
                .setTimestamp(Date.from(Instant.ofEpochMilli(10)))
                .setMinVersion(MlConfigVersion.CURRENT)
                .setQuantiles(new Quantiles("other_job", Date.from(Instant.ofEpochMilli(10)), randomAlphaOfLength(20)))
                .build()
        );
        // Add a snapshot WITHOUT a min version.
        client().prepareIndex(AnomalyDetectorsIndex.jobResultsAliasedName("other_job"))
            .setId(ModelSnapshot.documentId("other_job", "11"))
            .setSource("""
                {"job_id":"other_job","snapshot_id":"11", "snapshot_doc_count":1,"retain":false}""", XContentType.JSON)
            .get();

        indicesAdmin().prepareRefresh(AnomalyDetectorsIndex.jobStateIndexPattern(), AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*")
            .get();

        PlainActionFuture<QueryPage<ModelSnapshot>> future = new PlainActionFuture<>();
        jobProvider.modelSnapshots(jobId, 0, 4, "9", "15", "", false, "snap_2,snap_1", null, future::onResponse, future::onFailure);
        List<ModelSnapshot> snapshots = future.actionGet().results();
        assertThat(snapshots.get(0).getSnapshotId(), equalTo("snap_2"));
        assertNull(snapshots.get(0).getQuantiles());
        assertThat(snapshots.get(1).getSnapshotId(), equalTo("snap_1"));
        assertNull(snapshots.get(1).getQuantiles());

        future = new PlainActionFuture<>();
        jobProvider.modelSnapshots(jobId, 0, 4, "9", "15", "", false, "snap_*", null, future::onResponse, future::onFailure);
        snapshots = future.actionGet().results();
        assertThat(snapshots.get(0).getSnapshotId(), equalTo("snap_2"));
        assertThat(snapshots.get(1).getSnapshotId(), equalTo("snap_1"));
        assertNull(snapshots.get(0).getQuantiles());
        assertNull(snapshots.get(1).getQuantiles());

        future = new PlainActionFuture<>();
        jobProvider.modelSnapshots(jobId, 0, 4, "9", "15", "", false, "snap_*,other_snap", null, future::onResponse, future::onFailure);
        snapshots = future.actionGet().results();
        assertThat(snapshots.get(0).getSnapshotId(), equalTo("snap_2"));
        assertThat(snapshots.get(1).getSnapshotId(), equalTo("snap_1"));
        assertThat(snapshots.get(2).getSnapshotId(), equalTo("other_snap"));

        future = new PlainActionFuture<>();
        jobProvider.modelSnapshots(jobId, 0, 4, "9", "15", "", false, "*", null, future::onResponse, future::onFailure);
        snapshots = future.actionGet().results();
        assertThat(snapshots.get(0).getSnapshotId(), equalTo("snap_2"));
        assertThat(snapshots.get(1).getSnapshotId(), equalTo("snap_1"));
        assertThat(snapshots.get(2).getSnapshotId(), equalTo("other_snap"));

        future = new PlainActionFuture<>();
        jobProvider.modelSnapshots("*", 0, 5, null, null, "min_version", false, null, null, future::onResponse, future::onFailure);
        snapshots = future.actionGet().results();
        assertThat(snapshots.get(0).getSnapshotId(), equalTo("11"));
        assertThat(snapshots.get(1).getSnapshotId(), equalTo("snap_1"));
        assertThat(snapshots.get(2).getSnapshotId(), equalTo("other_snap"));
        assertThat(snapshots.get(3).getSnapshotId(), equalTo("snap_2"));
        assertThat(snapshots.get(4).getSnapshotId(), equalTo("other_snap"));

        // assert that quantiles are not loaded
        assertNull(snapshots.get(0).getQuantiles());
        assertNull(snapshots.get(1).getQuantiles());
        assertNull(snapshots.get(2).getQuantiles());
        assertNull(snapshots.get(3).getQuantiles());
        assertNull(snapshots.get(4).getQuantiles());

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

        indicesAdmin().prepareRefresh(
            MlMetaIndex.indexName(),
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            AnomalyDetectorsIndex.jobResultsAliasedName(jobId)
        ).get();

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
        jobProvider.scheduledEventsForJob(jobId, jobGroups, query, ActionListener.wrap(params -> {
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

    private List<ScheduledEvent> getScheduledEvents(ScheduledEventsQueryBuilder query) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<ScheduledEvent>> searchResultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobProvider.scheduledEvents(query, ActionListener.wrap(params -> {
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
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.indexName());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(
                    Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")
                );
                indexRequest.source(event.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        BulkResponse response = bulkRequest.execute().actionGet();
        if (response.hasFailures()) {
            throw new IllegalStateException(Strings.toString(response));
        }
    }

    private void indexDataCounts(DataCounts counts, String jobId) throws InterruptedException {
        JobDataCountsPersister persister = new JobDataCountsPersister(client(), resultsPersisterService, auditor);
        persister.persistDataCounts(jobId, counts, true);
    }

    private void indexFilters(List<MlFilter> filters) throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (MlFilter filter : filters) {
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.indexName()).id(filter.documentId());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(
                    Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")
                );
                indexRequest.source(filter.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        bulkRequest.execute().actionGet();
    }

    private void indexModelSizeStats(ModelSizeStats modelSizeStats) {
        JobResultsPersister persister = new JobResultsPersister(
            new OriginSettingClient(client(), ClientHelper.ML_ORIGIN),
            resultsPersisterService
        );
        persister.persistModelSizeStats(modelSizeStats, () -> true);
    }

    private void indexModelSnapshot(ModelSnapshot snapshot) {
        JobResultsPersister persister = new JobResultsPersister(
            new OriginSettingClient(client(), ClientHelper.ML_ORIGIN),
            resultsPersisterService
        );
        persister.persistModelSnapshot(snapshot, WriteRequest.RefreshPolicy.IMMEDIATE, () -> true);
    }

    private void indexQuantiles(Quantiles quantiles) {
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        createStateIndexAndAliasIfNecessary(
            client(),
            ClusterState.EMPTY_STATE,
            TestIndexNameExpressionResolver.newInstance(),
            MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
            future
        );
        future.actionGet();
        JobResultsPersister persister = new JobResultsPersister(
            new OriginSettingClient(client(), ClientHelper.ML_ORIGIN),
            resultsPersisterService
        );
        persister.persistQuantiles(quantiles, () -> true);
    }

    private void indexCalendars(List<Calendar> calendars) throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (Calendar calendar : calendars) {
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.indexName()).id(calendar.documentId());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ToXContent.MapParams params = new ToXContent.MapParams(
                    Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")
                );
                indexRequest.source(calendar.toXContent(builder, params));
                bulkRequest.add(indexRequest);
            }
        }
        bulkRequest.execute().actionGet();
    }
}
