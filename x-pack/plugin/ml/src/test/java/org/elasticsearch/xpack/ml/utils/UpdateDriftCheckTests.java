/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class UpdateDriftCheckTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(UpdateDriftCheckTests.class);
    private static final String LOGGER_NAME = UpdateDriftCheckTests.class.getCanonicalName();

    private final UpdateDriftCheck datafeedCheck = UpdateDriftCheck.forDatafeeds();
    private final UpdateDriftCheck jobCheck = UpdateDriftCheck.forJobs();

    private static MockLog.PatternSeenEventExpectation seenError(String name, String regex) {
        return new MockLog.PatternSeenEventExpectation(name, LOGGER_NAME, Level.ERROR, regex);
    }

    private static MockLog.LoggingExpectation[] silent(String name) {
        return new MockLog.LoggingExpectation[] {
            new MockLog.PatternNotSeenEventExpectation(name + " at ERROR", LOGGER_NAME, Level.ERROR, ".*"),
            new MockLog.PatternNotSeenEventExpectation(name + " at WARN", LOGGER_NAME, Level.WARN, ".*") };
    }

    // ---- violation shape: an update of one field silently resets another ----

    public void testDatafeedUpdateResettingUnrequestedFieldShouldLogViolationNamingFieldOnly() throws IOException {
        DatafeedConfig before = searchDatafeed().build();
        DatafeedUpdate update = new DatafeedUpdate.Builder("df-1").setFrequency(TimeValue.timeValueMinutes(7)).build();
        DatafeedConfig applied = update.apply(before, Map.of(), ClusterState.EMPTY_STATE);
        DatafeedConfig wrongMerge = new DatafeedConfig.Builder(applied).setScrollSize(1000).build();

        MockLog.assertThatLogger(
            () -> datafeedCheck.check(logger, "df-1", before, wrongMerge, update),
            UpdateDriftCheckTests.class,
            seenError(
                "scroll_size reported with token",
                "\\[ml-invariant:update_changed_unrequested_field\\] .*datafeed \\[df-1\\].*\\[scroll_size\\]"
            ),
            new MockLog.PatternNotSeenEventExpectation("values never logged", LOGGER_NAME, Level.ERROR, ".*(777|1000).*")
        );
    }

    public void testDatafeedUpdateChangingSeveralUnrequestedFieldsShouldNameThemSorted() throws IOException {
        DatafeedConfig before = searchDatafeed().build();
        DatafeedUpdate update = new DatafeedUpdate.Builder("df-1").setFrequency(TimeValue.timeValueMinutes(7)).build();
        DatafeedConfig applied = update.apply(before, Map.of(), ClusterState.EMPTY_STATE);
        DatafeedConfig wrongMerge = new DatafeedConfig.Builder(applied).setScrollSize(1000)
            .setQueryDelay(TimeValue.timeValueSeconds(5))
            .build();

        MockLog.assertThatLogger(
            () -> datafeedCheck.check(logger, "df-1", before, wrongMerge, update),
            UpdateDriftCheckTests.class,
            seenError("fields listed in order", ".*\\[query_delay, scroll_size\\]")
        );
    }

    public void testJobUpdateResettingUnrequestedFieldShouldLogViolationNamingFieldOnly() {
        Job before = richJob().build();
        JobUpdate update = new JobUpdate.Builder("job-1").setRenormalizationWindowDays(9L).build();
        Job applied = update.mergeWithJob(before, null);
        Job.Builder wrongBuilder = new Job.Builder(applied);
        wrongBuilder.setDescription(null);
        Job wrongMerge = wrongBuilder.build();

        MockLog.assertThatLogger(
            () -> jobCheck.check(logger, "job-1", before, wrongMerge, update),
            UpdateDriftCheckTests.class,
            seenError("description reported", "\\[ml-invariant:update_changed_unrequested_field\\] .*job \\[job-1\\].*\\[description\\]"),
            new MockLog.PatternNotSeenEventExpectation("values never logged", LOGGER_NAME, Level.ERROR, ".*original description.*")
        );
    }

    public void testJobUpdateResettingNestedAnalysisConfigFieldShouldNameNestedPath() {
        Job before = richJob().build();
        JobUpdate update = new JobUpdate.Builder("job-1").setDescription("changed").build();
        Job applied = update.mergeWithJob(before, null);
        Job wrongMerge = new Job.Builder(applied).setAnalysisConfig(
            new AnalysisConfig.Builder(applied.getAnalysisConfig()).setModelPruneWindow(null)
        ).build();

        MockLog.assertThatLogger(
            () -> jobCheck.check(logger, "job-1", before, wrongMerge, update),
            UpdateDriftCheckTests.class,
            seenError("nested path reported", ".*\\[analysis_config.model_prune_window\\]")
        );
    }

    public void testJobUpdateOfNestedFieldShouldNotExcuseOtherNestedFields() {
        Job before = richJob().build();
        JobUpdate update = new JobUpdate.Builder("job-1").setModelPruneWindow(TimeValue.timeValueDays(60)).build();
        Job applied = update.mergeWithJob(before, null);
        Job wrongMerge = new Job.Builder(applied).setAnalysisConfig(
            new AnalysisConfig.Builder(applied.getAnalysisConfig()).setInfluencers(List.of())
        ).build();

        MockLog.assertThatLogger(
            () -> jobCheck.check(logger, "job-1", before, wrongMerge, update),
            UpdateDriftCheckTests.class,
            seenError("sibling reported, requested field excused", ".*\\[analysis_config.influencers\\]")
        );
    }

    // ---- healthy operation: every updatable field, driven through the real apply/merge ----

    public void testDatafeedUpdateOfEachFieldShouldStaySilent() throws IOException {
        Map<String, DatafeedUpdate> updatesByField = datafeedUpdatesByField();
        for (Map.Entry<String, DatafeedUpdate> entry : updatesByField.entrySet()) {
            String field = entry.getKey();
            DatafeedUpdate update = entry.getValue();
            DatafeedConfig before = baselineFor(field);
            if (isRejectedOnEsqlDatafeed(field)) {
                expectThrows(ElasticsearchStatusException.class, () -> update.apply(before, Map.of(), ClusterState.EMPTY_STATE));
                continue;
            }
            DatafeedConfig after = update.apply(before, Map.of(), ClusterState.EMPTY_STATE);
            MockLog.assertThatLogger(
                () -> datafeedCheck.check(logger, "df-1", before, after, update),
                UpdateDriftCheckTests.class,
                silent("update of " + field)
            );
        }
    }

    public void testDatafeedUpdateDeclaredFieldsShouldAllHaveMatrixEntry() throws IOException {
        assertThat(datafeedUpdatesByField().keySet(), equalTo(declaredInstanceFields(DatafeedUpdate.class)));
    }

    public void testDatafeedUpdateResettingOptionalLimitsShouldStaySilent() throws IOException {
        DatafeedConfig before = searchDatafeed().build();
        DatafeedUpdate update = new DatafeedUpdate.Builder("df-1").setMaxEmptySearches(-1).setMaxConsecutiveExtractionFailures(-1).build();
        DatafeedConfig after = update.apply(before, Map.of(), ClusterState.EMPTY_STATE);

        MockLog.assertThatLogger(
            () -> datafeedCheck.check(logger, "df-1", before, after, update),
            UpdateDriftCheckTests.class,
            silent("reset of optional limits")
        );
    }

    public void testDatafeedUpdateRewritingHeadersShouldStaySilent() throws IOException {
        DatafeedConfig before = searchDatafeed().build();
        DatafeedUpdate update = new DatafeedUpdate.Builder("df-1").setFrequency(TimeValue.timeValueMinutes(7)).build();
        DatafeedConfig after = update.apply(before, Map.of("es-security-runas-user", "someone"), ClusterState.EMPTY_STATE);
        assertThat(after.getHeaders().isEmpty(), equalTo(false));

        MockLog.assertThatLogger(
            () -> datafeedCheck.check(logger, "df-1", before, after, update),
            UpdateDriftCheckTests.class,
            silent("header rewrite")
        );
    }

    public void testDatafeedUpdateOfAggregationDatafeedShouldStaySilent() throws IOException {
        DatafeedConfig before = aggregationDatafeed().build();
        DatafeedUpdate update = new DatafeedUpdate.Builder("df-1").setQueryDelay(TimeValue.timeValueSeconds(30)).build();
        DatafeedConfig after = update.apply(before, Map.of(), ClusterState.EMPTY_STATE);

        MockLog.assertThatLogger(
            () -> datafeedCheck.check(logger, "df-1", before, after, update),
            UpdateDriftCheckTests.class,
            silent("aggregation datafeed")
        );
    }

    public void testJobUpdateOfEachFieldShouldStaySilent() {
        Map<String, JobUpdate> updatesByField = jobUpdatesByField();
        for (Map.Entry<String, JobUpdate> entry : updatesByField.entrySet()) {
            String field = entry.getKey();
            JobUpdate update = entry.getValue();
            Job before = richJob().build();
            Job after = update.mergeWithJob(before, null);
            MockLog.assertThatLogger(
                () -> jobCheck.check(logger, "job-1", before, after, update),
                UpdateDriftCheckTests.class,
                silent("update of " + field)
            );
        }
    }

    public void testJobUpdateDeclaredFieldsShouldAllHaveMatrixEntry() {
        assertThat(jobUpdatesByField().keySet(), equalTo(declaredInstanceFields(JobUpdate.class)));
    }

    public void testJobUpdateOfMinimalJobShouldStaySilent() {
        Job.Builder builder = new Job.Builder("job-1");
        builder.setCreateTime(new Date(1_000_000L));
        builder.setAnalysisConfig(new AnalysisConfig.Builder(List.of(new Detector.Builder("count", null).build())));
        builder.setDataDescription(new DataDescription.Builder());
        Job before = builder.build();
        JobUpdate update = new JobUpdate.Builder("job-1").setDescription("added").build();
        Job after = update.mergeWithJob(before, null);

        MockLog.assertThatLogger(
            () -> jobCheck.check(logger, "job-1", before, after, update),
            UpdateDriftCheckTests.class,
            silent("minimal job")
        );
    }

    public void testDatafeedUpdateOfMinimalDatafeedShouldStaySilent() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df-1", "job-1");
        builder.setIndices(List.of("logs-*"));
        DatafeedConfig before = builder.build();
        DatafeedUpdate update = new DatafeedUpdate.Builder("df-1").setScrollSize(10).build();
        DatafeedConfig after = update.apply(before, Map.of(), ClusterState.EMPTY_STATE);

        MockLog.assertThatLogger(
            () -> datafeedCheck.check(logger, "df-1", before, after, update),
            UpdateDriftCheckTests.class,
            silent("minimal datafeed")
        );
    }

    // ---- fixtures ----

    @SuppressForbidden(reason = "reads field names only, so that a field added to an update class cannot skip the matrix")
    private static Set<String> declaredInstanceFields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
            .filter(f -> Modifier.isStatic(f.getModifiers()) == false && f.isSynthetic() == false)
            .map(Field::getName)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    private static boolean isRejectedOnEsqlDatafeed(String field) {
        return field.equals("esqlQuery") || field.equals("sourceTimeField") || field.equals("groupingInterval");
    }

    private static DatafeedConfig baselineFor(String field) throws IOException {
        if (isRejectedOnEsqlDatafeed(field)) {
            return esqlDatafeed().build();
        }
        return field.equals("aggProvider") ? aggregationDatafeed().build() : searchDatafeed().build();
    }

    private static DatafeedConfig.Builder searchDatafeed() throws IOException {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df-1", "job-1");
        builder.setIndices(List.of("logs-*"));
        builder.setQueryProvider(QueryProvider.fromParsedQuery(QueryBuilders.termQuery("host", "a")));
        builder.setFrequency(TimeValue.timeValueMinutes(2));
        builder.setQueryDelay(TimeValue.timeValueSeconds(90));
        builder.setScriptFields(List.of(new SearchSourceBuilder.ScriptField("sf", new Script("doc['x'].value"), false)));
        builder.setScrollSize(777);
        builder.setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueHours(1)));
        builder.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(2)));
        builder.setMaxEmptySearches(11);
        builder.setMaxConsecutiveExtractionFailures(5);
        builder.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        builder.setRuntimeMappings(Map.of("rt", Map.of("type", "keyword")));
        builder.setProjectRouting("_alias:*");
        return builder;
    }

    private static DatafeedConfig.Builder aggregationDatafeed() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df-1", "job-1");
        builder.setIndices(List.of("logs-*"));
        builder.setParsedAggregations(histogram("1h"));
        builder.setFrequency(TimeValue.timeValueHours(1));
        return builder;
    }

    private static DatafeedConfig.Builder esqlDatafeed() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df-1", "job-1");
        builder.setEsqlQuery("FROM logs-* | WHERE x > 0");
        builder.setSourceTimeField("time");
        builder.setGroupingInterval(TimeValue.timeValueMinutes(1));
        return builder;
    }

    private static AggregatorFactories.Builder histogram(String interval) {
        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders.dateHistogram("buckets")
            .field("time")
            .fixedInterval(new DateHistogramInterval(interval))
            .subAggregation(AggregationBuilders.max("time").field("time"));
        return new AggregatorFactories.Builder().addAggregator(dateHistogram);
    }

    private static Map<String, DatafeedUpdate> datafeedUpdatesByField() throws IOException {
        Map<String, DatafeedUpdate> updates = new TreeMap<>();
        updates.put("id", new DatafeedUpdate.Builder("df-1").build());
        updates.put("jobId", new DatafeedUpdate.Builder("df-1").setJobId("job-1").build());
        updates.put("queryDelay", new DatafeedUpdate.Builder("df-1").setQueryDelay(TimeValue.timeValueSeconds(30)).build());
        updates.put("frequency", new DatafeedUpdate.Builder("df-1").setFrequency(TimeValue.timeValueMinutes(7)).build());
        updates.put("indices", new DatafeedUpdate.Builder("df-1").setIndices(List.of("other-*")).build());
        updates.put(
            "queryProvider",
            new DatafeedUpdate.Builder("df-1").setQuery(QueryProvider.fromParsedQuery(QueryBuilders.termQuery("host", "b"))).build()
        );
        updates.put("esqlQuery", new DatafeedUpdate.Builder("df-1").setEsqlQuery("FROM other-* | WHERE y > 0").build());
        updates.put("sourceTimeField", new DatafeedUpdate.Builder("df-1").setSourceTimeField("other_time").build());
        updates.put("groupingInterval", new DatafeedUpdate.Builder("df-1").setGroupingInterval(TimeValue.timeValueMinutes(5)).build());
        updates.put("aggProvider", new DatafeedUpdate.Builder("df-1").setParsedAggregations(histogram("2h")).build());
        updates.put(
            "scriptFields",
            new DatafeedUpdate.Builder("df-1").setScriptFields(
                List.of(new SearchSourceBuilder.ScriptField("sf2", new Script("doc['y'].value"), false))
            ).build()
        );
        updates.put("scrollSize", new DatafeedUpdate.Builder("df-1").setScrollSize(123).build());
        updates.put("chunkingConfig", new DatafeedUpdate.Builder("df-1").setChunkingConfig(ChunkingConfig.newOff()).build());
        updates.put(
            "delayedDataCheckConfig",
            new DatafeedUpdate.Builder("df-1").setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig()).build()
        );
        updates.put("maxEmptySearches", new DatafeedUpdate.Builder("df-1").setMaxEmptySearches(22).build());
        updates.put("maxConsecutiveExtractionFailures", new DatafeedUpdate.Builder("df-1").setMaxConsecutiveExtractionFailures(7).build());
        updates.put(
            "indicesOptions",
            new DatafeedUpdate.Builder("df-1").setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED).build()
        );
        updates.put(
            "runtimeMappings",
            new DatafeedUpdate.Builder("df-1").setRuntimeMappings(Map.of("rt2", Map.of("type", "long"))).build()
        );
        updates.put("projectRouting", new DatafeedUpdate.Builder("df-1").setProjectRouting("_origin").build());
        updates.put("forceRekeying", new DatafeedUpdate.Builder("df-1").setForceRekeying(true).build());
        return updates;
    }

    private static Job.Builder richJob() {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("mlcategory");
        detector.setPartitionFieldName("host");
        detector.setDetectorDescription("original detector description");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(List.of(detector.build()));
        analysisConfig.setCategorizationFieldName("message");
        analysisConfig.setCategorizationFilters(List.of("foo"));
        analysisConfig.setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig(true, false));
        analysisConfig.setInfluencers(List.of("host"));
        analysisConfig.setModelPruneWindow(TimeValue.timeValueDays(30));

        Job.Builder job = new Job.Builder("job-1");
        job.setCreateTime(new Date(1_000_000L));
        job.setFinishedTime(new Date(2_000_000L));
        job.setJobVersion(MlConfigVersion.V_7_0_0);
        job.setGroups(List.of("original-group"));
        job.setDescription("original description");
        job.setAnalysisConfig(analysisConfig);
        job.setAnalysisLimits(new AnalysisLimits(512L, 4L));
        job.setDataDescription(new DataDescription.Builder());
        job.setModelPlotConfig(new ModelPlotConfig(false, null, false));
        job.setRenormalizationWindowDays(3L);
        job.setBackgroundPersistInterval(TimeValue.timeValueHours(2));
        job.setModelSnapshotRetentionDays(10L);
        job.setDailyModelSnapshotRetentionAfterDays(5L);
        job.setResultsRetentionDays(60L);
        job.setCustomSettings(Map.of("k", "original"));
        job.setModelSnapshotId("snap-1");
        job.setModelSnapshotMinVersion(MlConfigVersion.V_7_0_0);
        job.setAllowLazyOpen(false);
        return job;
    }

    private static Map<String, JobUpdate> jobUpdatesByField() {
        Map<String, JobUpdate> updates = new TreeMap<>();
        updates.put("jobId", new JobUpdate.Builder("job-1").build());
        updates.put("groups", new JobUpdate.Builder("job-1").setGroups(List.of("new-group")).build());
        updates.put("description", new JobUpdate.Builder("job-1").setDescription("new description").build());
        updates.put(
            "detectorUpdates",
            new JobUpdate.Builder("job-1").setDetectorUpdates(List.of(new JobUpdate.DetectorUpdate(0, "new detector description", null)))
                .build()
        );
        updates.put("modelPlotConfig", new JobUpdate.Builder("job-1").setModelPlotConfig(new ModelPlotConfig(true, "host", true)).build());
        updates.put("analysisLimits", new JobUpdate.Builder("job-1").setAnalysisLimits(new AnalysisLimits(1024L, 8L)).build());
        updates.put("renormalizationWindowDays", new JobUpdate.Builder("job-1").setRenormalizationWindowDays(9L).build());
        updates.put(
            "backgroundPersistInterval",
            new JobUpdate.Builder("job-1").setBackgroundPersistInterval(TimeValue.timeValueHours(4)).build()
        );
        updates.put("modelSnapshotRetentionDays", new JobUpdate.Builder("job-1").setModelSnapshotRetentionDays(20L).build());
        updates.put(
            "dailyModelSnapshotRetentionAfterDays",
            new JobUpdate.Builder("job-1").setDailyModelSnapshotRetentionAfterDays(6L).build()
        );
        updates.put("resultsRetentionDays", new JobUpdate.Builder("job-1").setResultsRetentionDays(90L).build());
        updates.put("categorizationFilters", new JobUpdate.Builder("job-1").setCategorizationFilters(List.of("bar")).build());
        updates.put(
            "perPartitionCategorizationConfig",
            new JobUpdate.Builder("job-1").setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig(true, true)).build()
        );
        updates.put("customSettings", new JobUpdate.Builder("job-1").setCustomSettings(Map.of("k", "new")).build());
        updates.put("modelSnapshotId", new JobUpdate.Builder("job-1").setModelSnapshotId("snap-2").build());
        updates.put("modelSnapshotMinVersion", new JobUpdate.Builder("job-1").setModelSnapshotMinVersion(MlConfigVersion.CURRENT).build());
        updates.put("jobVersion", new JobUpdate.Builder("job-1").setJobVersion(MlConfigVersion.CURRENT).build());
        updates.put("clearJobFinishTime", new JobUpdate.Builder("job-1").setClearFinishTime(true).build());
        updates.put("allowLazyOpen", new JobUpdate.Builder("job-1").setAllowLazyOpen(true).build());
        updates.put("blocked", new JobUpdate.Builder("job-1").setBlocked(new Blocked(Blocked.Reason.RESET, null)).build());
        updates.put("modelPruneWindow", new JobUpdate.Builder("job-1").setModelPruneWindow(TimeValue.timeValueDays(60)).build());
        return updates;
    }
}
