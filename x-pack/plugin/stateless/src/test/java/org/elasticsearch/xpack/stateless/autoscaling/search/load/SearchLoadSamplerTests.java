/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search.load;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class SearchLoadSamplerTests extends ESTestCase {
    record PublishedMetric(int sampleNum, boolean hasLoad, MetricQuality quality) {}

    public void testPublishingQuality() {
        // Alternates between EXACT and MINIMUM quality.
        class TestMetricQualitySupplier implements Supplier<MetricQuality> {
            int count = 0;

            @Override
            public MetricQuality get() {
                var quality = (count % 2 == 0) ? MetricQuality.EXACT : MetricQuality.MINIMUM;
                count++;
                return quality;
            }
        }
        assertQuality(
            2,
            2,
            new TestMetricQualitySupplier(),
            List.of(
                // 1. Initial metric warrants publish.
                new PublishedMetric(1, true, MetricQuality.EXACT),
                // 2. Second metric quality is MINIMUM, max cycles exceeded: publish.
                new PublishedMetric(2, false, MetricQuality.MINIMUM)
            )
        );
        assertQuality(
            2,
            3,
            new TestMetricQualitySupplier(),
            List.of(
                // 1. Initial metric warrants publish
                new PublishedMetric(1, true, MetricQuality.EXACT)
            // 2. Second metric quality is MINIMUM, max cycles not exceeded: don't publish
            )
        );
        assertQuality(
            3,
            3,
            new TestMetricQualitySupplier(),
            List.of(
                // 1. Initial metric warrants publish
                new PublishedMetric(1, true, MetricQuality.EXACT),
                // 2. Second metric quality is MINIMUM, max cycles not exceeded: don't publish
                // 3. Second metric quality is EXACT, max cycles exceeded: publish
                new PublishedMetric(3, true, MetricQuality.EXACT)
            )
        );
        assertQuality(
            4,
            4,
            new TestMetricQualitySupplier(),
            List.of(
                // 1. Initial metric warrants publish
                new PublishedMetric(1, true, MetricQuality.EXACT),
                // 2. Second metric quality is MINIMUM, max cycles not exceeded: don't publish
                // 3. Second metric quality is EXACT, max cycles not exceeded since last EXACT publish, sensitivity threshold
                // not exceeded: don't publish
                // 4. Max samples between publishing exceeded, metric quality MINIMUM: publish
                new PublishedMetric(4, false, MetricQuality.MINIMUM)
            )
        );
        assertQuality(
            5,
            4,
            new TestMetricQualitySupplier(),
            List.of(
                // 1. Initial metric warrants publish
                new PublishedMetric(1, true, MetricQuality.EXACT),
                // 2. Second metric quality is MINIMUM, max cycles not exceeded: don't publish
                // 3. Second metric quality is EXACT, max cycles not exceeded since last EXACT publish, sensitivity threshold
                // not exceeded: don't publish
                // 4. Max samples between publishing exceeded, metric quality MINIMUM: publish
                new PublishedMetric(4, false, MetricQuality.MINIMUM),
                // 5. Last published metric had 0 load, now non-zero: publish
                new PublishedMetric(5, true, MetricQuality.EXACT)
            )
        );
    }

    private void assertQuality(
        int cyclesToRunFor,
        int maxSamplesBetweenMetricPublications,
        Supplier<MetricQuality> metricQualitySupplier,
        List<PublishedMetric> expectedResults
    ) {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var minSensitivityRatio = 0.1;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(maxSamplesBetweenMetricPublications - 1);
        var clusterSettings = clusterSettings(
            Settings.builder()
                .put(SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING.getKey(), minSensitivityRatio)
                .put(SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), maxTimeBetweenMetricPublications)
                .build()
        );

        var numProcessors = randomIntBetween(2, 32);
        var lowestSearchLoadGTESensitivityRatio = numProcessors * minSensitivityRatio;
        var nodeSearchLoad = randomDoubleBetween(lowestSearchLoadGTESensitivityRatio, numProcessors, true);

        var averageSearchLoadSampler = new RandomAverageSearchLoadSampler(threadPool);

        var publishedMetrics = new ArrayList<PublishedMetric>();

        var sampler = new SearchLoadSampler(
            null,
            averageSearchLoadSampler,
            () -> nodeSearchLoad,
            metricQualitySupplier,
            numProcessors,
            clusterSettings,
            threadPool
        ) {
            @Override
            public void publishSearchLoad(double searchLoad, MetricQuality quality, String nodeId, ActionListener<Void> listener) {
                // publishedMetrics.add(Tuple.tuple(deterministicTaskQueue.getCurrentTimeMillis(), searchLoad));
                publishedMetrics.add(
                    new PublishedMetric(((int) deterministicTaskQueue.getCurrentTimeMillis() / 1000) + 1, searchLoad != 0.0, quality)
                );
                listener.onResponse(null);
            }
        };
        sampler.setNodeId(randomIdentifier());
        sampler.start();

        runFor(deterministicTaskQueue, (cyclesToRunFor - 1) * 1000L);

        assertThat(publishedMetrics, is(equalTo(expectedResults)));
    }

    public void testSearchLoadIsPublishedWithFixedFrequencyIfItDoesNotChange() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var minSensitivityRatio = 0.1;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(10);
        var clusterSettings = clusterSettings(
            Settings.builder()
                .put(SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING.getKey(), minSensitivityRatio)
                .put(SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), maxTimeBetweenMetricPublications)
                .build()
        );

        var numProcessors = randomIntBetween(2, 32);
        var nodeSearchLoad = randomSearchLoad(numProcessors);

        var publishedMetrics = new ArrayList<>();
        var searchLoadSampler = new RandomAverageSearchLoadSampler(threadPool);

        var sampler = new SearchLoadSampler(
            null,
            searchLoadSampler,
            () -> nodeSearchLoad,
            () -> MetricQuality.EXACT,
            numProcessors,
            clusterSettings,
            threadPool
        ) {
            @Override
            public void publishSearchLoad(double searchLoad, MetricQuality quality, String nodeId, ActionListener<Void> listener) {
                publishedMetrics.add(Tuple.tuple(deterministicTaskQueue.getCurrentTimeMillis(), searchLoad));
                listener.onResponse(null);
            }
        };
        sampler.setNodeId(randomIdentifier());
        sampler.start();

        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis());

        assertThat(
            publishedMetrics,
            is(equalTo(List.of(Tuple.tuple(0L, nodeSearchLoad), Tuple.tuple(maxTimeBetweenMetricPublications.millis(), nodeSearchLoad))))
        );
    }

    public void testMetricsArePublishedAlwaysWhenSensitivityIsZero() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var minSensitivityRatio = 0;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(10);
        var clusterSettings = clusterSettings(
            Settings.builder()
                .put(SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING.getKey(), minSensitivityRatio)
                .put(SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), maxTimeBetweenMetricPublications)
                .build()
        );

        var numProcessors = randomIntBetween(2, 32);

        // Increasing load
        var indexLoadOverTime = IntStream.range(0, 32).mapToDouble(l -> l).boxed().toList();
        var currentIndexLoadSupplier = indexLoadOverTime.iterator();

        var publishedMetrics = new ArrayList<Double>();
        var searchLoadSampler = new RandomAverageSearchLoadSampler(threadPool);

        var sampler = new SearchLoadSampler(
            null,
            searchLoadSampler,
            currentIndexLoadSupplier::next,
            () -> MetricQuality.EXACT,
            numProcessors,
            clusterSettings,
            threadPool

        ) {
            @Override
            public void publishSearchLoad(double searchLoad, MetricQuality quality, String nodeId, ActionListener<Void> listener) {
                publishedMetrics.add(searchLoad);
                listener.onResponse(null);
            }
        };
        sampler.setNodeId(randomIdentifier());
        sampler.start();

        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis());

        // The sampler publishes the first reading + publishes 1 reading per second until 10 second elapses
        assertThat(publishedMetrics, hasSize(11));
        assertThat(publishedMetrics, is(equalTo(indexLoadOverTime.subList(0, 11))));
    }

    public void testMetricsArePublishedWhenChangeIsAboveSensitivity() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        // TODO: maybe randomize numProcessors
        var minSensitivityRatio = 0.1;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(30);
        var clusterSettings = clusterSettings(
            Settings.builder()
                .put(SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING.getKey(), minSensitivityRatio)
                .put(SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), maxTimeBetweenMetricPublications)
                .build()
        );
        int numProcessors = 32;

        var changesAboveMinSensitivity = new AtomicInteger();
        var publishedMetrics = new ArrayList<Double>();
        DoubleSupplier searchLoadProbe = () -> {
            // First reading
            if (publishedMetrics.isEmpty()) {
                changesAboveMinSensitivity.incrementAndGet();
                return randomSearchLoad(numProcessors);
            }

            var latestPublishedMetric = publishedMetrics.get(publishedMetrics.size() - 1);
            if (randomBoolean()) {
                double loadChangeAboveSensitivity = randomDoubleBetween(numProcessors * minSensitivityRatio, numProcessors, true);

                if (randomBoolean()) {
                    changesAboveMinSensitivity.incrementAndGet();
                    return latestPublishedMetric + loadChangeAboveSensitivity;
                } else {
                    return Math.max(latestPublishedMetric - loadChangeAboveSensitivity, 0);
                }
            }
            return latestPublishedMetric;
        };

        var searchLoadSampler = new RandomAverageSearchLoadSampler(threadPool);

        var sampler = new SearchLoadSampler(
            null,
            searchLoadSampler,
            searchLoadProbe,
            () -> MetricQuality.EXACT,
            numProcessors,
            clusterSettings,
            threadPool
        ) {
            @Override
            public void publishSearchLoad(double searchLoad, MetricQuality quality, String nodeId, ActionListener<Void> listener) {
                publishedMetrics.add(searchLoad);
                listener.onResponse(null);
            }
        };
        sampler.setNodeId(randomIdentifier());
        sampler.start();

        // Run for less than maxTimeBetweenMetricPublications to ensure that we only account
        // for the metrics published due to a significant change
        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis() - TimeValue.timeValueSeconds(1).millis());

        // Account for initial publication
        assertThat(publishedMetrics, hasSize(changesAboveMinSensitivity.get()));
    }

    public void testMetricsArePublishedWhenChangeIsAboveSensitivityInIncrementsBelowSensitivity() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var minSensitivityRatio = 0.1;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(10);
        var clusterSettings = clusterSettings(
            Settings.builder()
                .put(SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING.getKey(), minSensitivityRatio)
                .put(SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), maxTimeBetweenMetricPublications)
                .build()
        );
        int numProcessors = 32;

        var searchLoadsOverTime = IntStream.range(0, 15).mapToObj(n -> n * numProcessors * (minSensitivityRatio - 0.01)).toList();
        var readingIter = searchLoadsOverTime.iterator();

        var publishedMetrics = new ArrayList<Double>();
        var searchLoadSampler = new RandomAverageSearchLoadSampler(threadPool);

        var sampler = new SearchLoadSampler(
            null,
            searchLoadSampler,
            readingIter::next,
            () -> MetricQuality.EXACT,
            numProcessors,
            clusterSettings,
            threadPool
        ) {
            @Override
            public void publishSearchLoad(double searchLoad, MetricQuality quality, String nodeId, ActionListener<Void> listener) {
                publishedMetrics.add(searchLoad);
                listener.onResponse(null);
            }
        };
        sampler.setNodeId(randomIdentifier());
        sampler.start();

        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis());

        // | time | search_load | ratio_diff | latest_published_search_load | publish |
        // | 0 | 0 | 0 | 0 | true |
        // | 1 | 2.88 | 0.09 | 0 | false |
        // | 2 | 5.76 | 0.18 | 0 | true |
        // | 3 | 8.64 | 0.09 | 5.76 | false |
        // | 4 | 11.52 | 0.18 | 5.76 | true |
        // | 5 | 14.40 | 0.09 | 11.52 | false |
        // | 6 | 17.28 | 0.18 | 11.52 | true |
        // | 7 | 20.16 | 0.09 | 17.28 | false |
        // | 8 | 23.04 | 0.18 | 17.28 | true |
        // | 9 | 25.92 | 0.09 | 23.04 | false |
        // | 10 | 28.80 | 0.18 | 23.04 | true |
        assertThat(publishedMetrics.toString(), publishedMetrics, hasSize(6));
    }

    public void testMetricsAreNotPublishedWhenChangeIsBelowSensitivity() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var minSensitivityRatio = 0.1;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(10);
        var clusterSettings = clusterSettings(
            Settings.builder()
                .put(SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING.getKey(), minSensitivityRatio)
                .put(SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), maxTimeBetweenMetricPublications)
                .build()
        );
        int numProcessors = 32;

        var publishedMetrics = new ArrayList<Double>();
        var significantLoadChangeCount = new AtomicInteger();

        DoubleSupplier currentIndexLoadSupplier = () -> {
            // first sample
            if (publishedMetrics.isEmpty()) {
                significantLoadChangeCount.incrementAndGet();
                return randomSearchLoad(numProcessors);
            }

            var previousReading = publishedMetrics.get(publishedMetrics.size() - 1);

            double loadChangeBelowSensitivity = randomDoubleBetween(0, numProcessors * (0.1 - Math.ulp(numProcessors * 0.1)), true);

            if (previousReading == 0) {
                return loadChangeBelowSensitivity;
            }

            return randomBoolean()
                ? previousReading + loadChangeBelowSensitivity
                : Math.max(previousReading - loadChangeBelowSensitivity, 0);
        };

        var searchLoadSampler = new RandomAverageSearchLoadSampler(threadPool);

        var sampler = new SearchLoadSampler(
            null,
            searchLoadSampler,
            currentIndexLoadSupplier,
            () -> MetricQuality.EXACT,
            numProcessors,
            clusterSettings,
            threadPool
        ) {
            @Override
            public void publishSearchLoad(double searchLoad, MetricQuality quality, String nodeId, ActionListener<Void> listener) {
                publishedMetrics.add(searchLoad);
                listener.onResponse(null);
            }
        };
        sampler.setNodeId(randomIdentifier());
        sampler.start();

        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis());

        // Initial reading + maxTimeBetweenPublications
        assertThat(publishedMetrics.toString(), publishedMetrics, hasSize(2));
    }

    public void testSamplingStopsWhenTheServiceIsStopped() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var indexLoad = randomSearchLoad(randomIntBetween(2, 32));

        var publishedMetrics = new ArrayList<Double>();
        var searchLoadSampler = new RandomAverageSearchLoadSampler(threadPool);

        var clusterSettings = clusterSettings(Settings.EMPTY);
        var sampler = new SearchLoadSampler(
            null,
            searchLoadSampler,
            () -> indexLoad,
            () -> MetricQuality.EXACT,
            8,
            clusterSettings,
            threadPool
        ) {
            @Override
            public void publishSearchLoad(double searchLoad, MetricQuality quality, String nodeId, ActionListener<Void> listener) {
                publishedMetrics.add(searchLoad);
                listener.onResponse(null);
            }
        };
        sampler.setNodeId(randomIdentifier());

        // The sampler does not schedule any task before starting the service or publish any metrics
        assertThat(publishedMetrics, is(empty()));
        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(false));

        sampler.start();

        // It publishes the first reading and schedules the next sampling task
        assertThat(publishedMetrics, is(equalTo(List.of(indexLoad))));
        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(true));

        sampler.stop();

        // After stopping the service; it won't schedule more sampling tasks
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runRandomTask();
        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(false));

        assertThat(publishedMetrics, is(equalTo(List.of(indexLoad))));
    }

    public void testMetricsArePublishedAfterFailures() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var minSensitivityRatio = 0;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(10);
        var clusterSettings = clusterSettings(
            Settings.builder()
                .put(SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING.getKey(), minSensitivityRatio)
                .put(SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.getKey(), samplingFrequency)
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), maxTimeBetweenMetricPublications)
                .build()
        );

        var numProcessors = randomIntBetween(2, 32);

        // Increasing load
        var indexLoadOverTime = IntStream.range(0, 32).mapToDouble(l -> l).boxed().toList();
        var currentIndexLoadSupplier = indexLoadOverTime.iterator();

        var publishedMetrics = new ArrayList<Double>();
        var publicationFails = new AtomicBoolean(true);
        var publicationFailures = new AtomicInteger();
        var searchLoadSampler = new RandomAverageSearchLoadSampler(threadPool);

        var sampler = new SearchLoadSampler(
            null,
            searchLoadSampler,
            currentIndexLoadSupplier::next,
            () -> MetricQuality.EXACT,
            numProcessors,
            clusterSettings,
            threadPool
        ) {
            @Override
            public void publishSearchLoad(double searchLoad, MetricQuality quality, String nodeId, ActionListener<Void> listener) {
                if (publicationFails.get()) {
                    publicationFailures.incrementAndGet();
                    listener.onFailure(new IllegalArgumentException("Boom"));
                } else {
                    publishedMetrics.add(searchLoad);
                    listener.onResponse(null);
                }
            }
        };
        sampler.setNodeId(randomIdentifier());
        sampler.start();

        runFor(deterministicTaskQueue, samplingFrequency.millis());
        // It tries to publish the first reading + 1 reading per second
        assertThat(publicationFailures.get(), is(equalTo(2)));
        publicationFails.set(false);

        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis());
        // The sampler publishes the first reading + publishes 1 reading per second until 10 second elapses
        assertThat(publishedMetrics, hasSize(11));
        assertThat(publishedMetrics, is(equalTo(indexLoadOverTime.subList(2, 13))));
    }

    private void runFor(DeterministicTaskQueue deterministicTaskQueue, long runDurationMillis) {
        var endTime = deterministicTaskQueue.getCurrentTimeMillis() + runDurationMillis;

        while (deterministicTaskQueue.getCurrentTimeMillis() <= endTime) {
            while (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }

            assertThat(deterministicTaskQueue.hasDeferredTasks(), is(true));
            deterministicTaskQueue.advanceTime();
        }
    }

    private double randomSearchLoad(int bound) {
        return randomDoubleBetween(0, bound, true);
    }

    // A mocked sampler that returns random values
    private static class RandomAverageSearchLoadSampler extends AverageSearchLoadSampler {
        RandomAverageSearchLoadSampler(ThreadPool threadPool) {
            super(
                threadPool,
                TimeValue.timeValueSeconds(1),
                Map.of(SEARCH_EXECUTOR, DEFAULT_SEARCH_EWMA_ALPHA, SHARD_READ_EXECUTOR, DEFAULT_SHARD_READ_EWMA_ALPHA),
                4
            );
        }

        @Override
        public void sample() {}

        @Override
        public ExecutorLoadStats getExecutorLoadStats(String executor) {
            return new ExecutorLoadStats(
                randomDoubleBetween(0.0, 8.0, true),
                randomDoubleBetween(100.0, 500.0, true),
                randomIntBetween(0, 100),
                between(5, 10),
                between(1, 15)
            );
        }
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(
            settings,
            Set.of(
                SearchLoadSampler.SAMPLING_FREQUENCY_SETTING,
                SearchLoadSampler.MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING,
                SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING
            )
        );
    }
}
