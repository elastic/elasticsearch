/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.querysampling.capture.CaptureHandoff;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureFilter;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.groundtruth.CostBudget;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruthWorker;
import org.elasticsearch.xpack.querysampling.sampling.EventSlice;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.storage.QuerySamplingIndex;
import org.elasticsearch.xpack.querysampling.storage.SampleRetention;
import org.elasticsearch.xpack.querysampling.storage.SampleWriter;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;
import org.elasticsearch.xpack.querysampling.storage.WeightsRefresher;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.sameInstance;

public class QuerySamplingTests extends ESTestCase {

    private final MultiplicityTracker tracker = new MultiplicityTracker(100);

    private final List<SampledQuery> sampled = new ArrayList<>();

    public void testRepeatedQueryIsPickedOnceAndKeepsBeingCounted() {
        SamplingPipeline pipeline = pipeline(new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        });
        float[] vector = { 1f, 2f, 3f };

        for (int i = 0; i < 5; i++) {
            pipeline.accept(search(vector));
        }

        assertThat(sampled.size(), equalTo(1));
        assertThat(pipeline.picked(), equalTo(1L));
        assertThat(tracker.distinct(), equalTo(1));
    }

    public void testDistinctQueriesArePickedSeparately() {
        SamplingPipeline pipeline = pipeline(new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        });

        pipeline.accept(search(new float[] { 1f }));
        pipeline.accept(search(new float[] { 2f }));
        pipeline.accept(search(new float[] { 3f }));

        assertThat(sampled.size(), equalTo(3));
        assertThat(pipeline.picked(), equalTo(3L));
        assertThat(tracker.distinct(), equalTo(3));
    }

    public void testNothingIsPassedOnWhenNothingIsPicked() {
        SamplingPipeline pipeline = pipeline(new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.999999;
            }
        });

        for (int i = 0; i < 10; i++) {
            pipeline.accept(search(new float[] { i }));
        }

        assertThat(sampled.size(), equalTo(0));
        assertThat(pipeline.picked(), equalTo(0L));
        assertThat(tracker.distinct(), equalTo(10));
    }

    public void testEveryListenerIsToldAndAFailingOneDoesNotStopTheOthers() {
        List<SampledQuery> first = new ArrayList<>();
        List<SampledQuery> last = new ArrayList<>();
        SamplingPipeline pipeline = new SamplingPipeline(tracker, new QuerySampler(1.0, 100, new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        }), List.of(first::add, query -> { throw new IllegalStateException("listener failed"); }, last::add));

        pipeline.accept(search(new float[] { 1f }));

        assertThat(first.size(), equalTo(1));
        assertThat(last.size(), equalTo(1));
        assertThat(last.get(0), sameInstance(first.get(0)));
    }

    public void testCapturedSearchesEarnTheTimeOfTheSearchesTheyStandFor() {
        CostBudget budget = new CostBudget(0.5, 1_000_000);
        SamplingPipeline pipeline = new SamplingPipeline(tracker, new QuerySampler(1.0, 100, new Random(0L)), List.of(), budget);
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);

        pipeline.accept(new CapturedSearch(query, List.of(), 10, 0.1));

        assertThat(budget.credit(), closeTo(0.5 * 100, 1e-9));
    }

    public void testSomeCapturedSearchesAreAlsoKeptAsEventsWhateverHappensToTheQuery() {
        EventSlice events = new EventSlice(() -> new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        });
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(QuerySamplingSettings.EVENT_SLICE_RATE.getKey(), 0.25).build(),
            Set.of(QuerySamplingSettings.EVENT_SLICE_RATE)
        );
        events.watch(clusterSettings);
        MultiplicityTracker tiny = new MultiplicityTracker(1);
        SamplingPipeline pipeline = new SamplingPipeline(tiny, new QuerySampler(0.1, 100, new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.999999;
            }
        }), List.of(sampled::add), new CostBudget(0.0, 0.0), events);
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);

        pipeline.accept(new CapturedSearch(query, List.of(), 1, 0.5));
        pipeline.accept(new CapturedSearch(query, List.of(), 1, 0.5));
        pipeline.accept(search(new float[] { 2f }));

        assertThat(pipeline.picked(), equalTo(0L));
        assertThat(pipeline.eventsKept(), equalTo(3L));
        assertThat(sampled.size(), equalTo(3));
        assertTrue(sampled.stream().allMatch(SampledQuery::isEvent));
        assertThat("two events of the same query", sampled.get(0).eventId().equals(sampled.get(1).eventId()), equalTo(false));
        assertThat("captured with 0.5 and kept with 0.25", sampled.get(0).tracked().inclusionProbability(), closeTo(0.125, 1e-12));
        assertThat("each is one search, standing for two", sampled.get(0).tracked().weightedMultiplicity(), closeTo(2.0, 1e-12));
    }

    private SamplingPipeline pipeline(Random random) {
        return new SamplingPipeline(tracker, new QuerySampler(1.0, 100, random), List.of(sampled::add));
    }

    private static CapturedSearch search(float[] vector) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", vector, 10, 100, null, null, List.of(), null);
        return new CapturedSearch(query, List.of(), 1, 1.0);
    }

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

    private final MultiplicityTracker querySamplingServiceTracker = new MultiplicityTracker(2);

    private final SampleWriter writer = new SampleWriter(
        "sampler",
        (request, listener) -> listener.onResponse(allWritten(request.numberOfActions())),
        taskQueue.getThreadPool(),
        taskQueue.getThreadPool().generic(),
        () -> 42L,
        (fingerprint, tracked, weights) -> {},
        2,
        10,
        TimeValue.timeValueSeconds(1)
    );

    private final WeightsRefresher refresher = new WeightsRefresher(
        "sampler",
        (request, listener) -> {},
        taskQueue.getThreadPool(),
        taskQueue.getThreadPool().generic(),
        () -> 42L,
        (fingerprint, tracked) -> true,
        10,
        TimeValue.timeValueSeconds(30)
    );

    private final CostBudget budget = new CostBudget(0.0, 1000);

    private final GroundTruthWorker groundTruthWorker = new GroundTruthWorker(
        (request, listener) -> {},
        (request, listener) -> {},
        (request, listener) -> {},
        NamedXContentRegistry.EMPTY,
        budget,
        "sampler",
        () -> 42L
    );

    private final SampleRetention retention = new SampleRetention(
        (request, listener) -> {},
        () -> true,
        () -> 42L,
        TimeValue.timeValueDays(7)
    );

    public void testGateCountersOnlyCountEligibleKnnSearches() {
        QueryCaptureFilter filter = filter(1.0);
        QuerySamplingService service = service(filter, handoff(Runnable::run, new Random(0L)));

        querySamplingServiceSearch(filter, knnSearch());
        querySamplingServiceSearch(filter, knnSearch());
        querySamplingServiceSearch(filter, new SearchRequest("idx").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())));

        QuerySamplingStats stats = service.stats();
        assertThat(stats.knnSearches(), equalTo(2L));
        assertThat(stats.captured(), equalTo(2L));
    }

    public void testStagesAreReflectedInTheStats() {
        CaptureHandoff handoff = handoff(Runnable::run, picking());
        QuerySamplingService service = service(filter(1.0), handoff);

        handoff.accept(querySamplingServiceSearch(1f));
        handoff.accept(querySamplingServiceSearch(2f));
        handoff.accept(querySamplingServiceSearch(3f));
        handoff.accept(querySamplingServiceSearch(1f));
        taskQueue.runAllRunnableTasks();

        QuerySamplingStats stats = service.stats();
        assertThat(stats.distinctQueries(), equalTo(2L));
        assertThat(stats.untrackedArrivals(), equalTo(1L));
        assertThat(stats.picked(), equalTo(2L));
        assertThat("both picked queries went out in one batch", stats.written(), equalTo(2L));
        assertThat(stats.writeFailures(), equalTo(0L));
        assertThat(stats.writeDropped(), equalTo(0L));
        assertThat(stats.dropped(), equalTo(0L));
    }

    private SamplingPipeline querySamplingServicePipeline;

    private CaptureHandoff handoff(Executor executor, Random random) {
        querySamplingServicePipeline = new SamplingPipeline(
            querySamplingServiceTracker,
            new QuerySampler(1.0, 100, random),
            List.of(writer)
        );
        return new CaptureHandoff(executor, querySamplingServicePipeline);
    }

    private QuerySamplingService service(QueryCaptureFilter filter, CaptureHandoff handoff) {
        return new QuerySamplingService(
            filter,
            handoff,
            querySamplingServiceTracker,
            querySamplingServicePipeline,
            writer,
            refresher,
            retention,
            groundTruthWorker,
            budget
        );
    }

    private static BulkResponse allWritten(int queries) {
        BulkItemResponse[] items = new BulkItemResponse[queries];
        for (int i = 0; i < queries; i++) {
            items[i] = BulkItemResponse.success(
                i,
                DocWriteRequest.OpType.INDEX,
                new IndexResponse(new ShardId(QuerySamplingIndex.NAME, "_na_", 0), "id", 0, 1, 1, true)
            );
        }
        return new BulkResponse(items, 1);
    }

    private static Random picking() {
        return new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        };
    }

    private static QueryCaptureFilter filter(double captureRate) {
        Settings settings = Settings.builder()
            .put(QuerySamplingSettings.ENABLED.getKey(), true)
            .put(QuerySamplingSettings.CAPTURE_RATE.getKey(), captureRate)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(QuerySamplingSettings.ENABLED, QuerySamplingSettings.CAPTURE_RATE, QuerySamplingSettings.MIN_CAPTURES_PER_HOUR)
        );
        return new QueryCaptureFilter(clusterSettings, captured -> {});
    }

    private static void querySamplingServiceSearch(QueryCaptureFilter filter, SearchRequest request) {
        Task task = new Task(1, "transport", TransportSearchAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
        filter.apply(task, TransportSearchAction.NAME, request, ActionListener.<SearchResponse>noop(), (t, action, r, listener) -> {});
    }

    private static SearchRequest knnSearch() {
        KnnSearchBuilder knn = new KnnSearchBuilder("vec", new float[] { randomFloat(), randomFloat() }, 10, 100, null, null, null);
        return new SearchRequest("idx").source(new SearchSourceBuilder().knnSearch(List.of(knn)));
    }

    private static CapturedSearch querySamplingServiceSearch(float value) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { value }, 10, 100, null, null, List.of(), null);
        return new CapturedSearch(query, List.of(), 1, 1.0);
    }

    public void testEveryDeclaredSettingIsRegistered() throws IllegalAccessException {
        int declared = 0;
        for (Field field : QuerySamplingSettings.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) && Setting.class.isAssignableFrom(field.getType())) {
                declared++;
                assertThat(field.getName() + " is registered", QuerySamplingSettings.getSettings(), hasItem((Setting<?>) field.get(null)));
            }
        }
        assertThat(declared, equalTo(QuerySamplingSettings.getSettings().size()));
    }

    private static void expectInvalid(Setting<?> setting, String value) {
        expectThrows(IllegalArgumentException.class, () -> setting.get(Settings.builder().put(setting.getKey(), value).build()));
    }
}
