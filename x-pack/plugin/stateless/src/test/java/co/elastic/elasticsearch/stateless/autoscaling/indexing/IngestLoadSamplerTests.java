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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class IngestLoadSamplerTests extends ESTestCase {
    public void testIngestionLoadIsPublishedWithFixedFrequencyIfItDoesNotChange() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var minSensitivityRatio = 0.1;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(10);
        var numProcessors = randomIntBetween(2, 32);
        var nodeIngestLoad = randomIngestionLoad(numProcessors);

        var publishedMetrics = new ArrayList<>();
        var ingestLoadPublisher = new IngestLoadPublisher(null, (Supplier<String>) null, null) {
            @Override
            public void publishIngestionLoad(double ingestionLoad, ActionListener<Void> listener) {
                publishedMetrics.add(Tuple.tuple(deterministicTaskQueue.getCurrentTimeMillis(), ingestionLoad));
                listener.onResponse(null);
            }
        };

        new IngestLoadSampler(
            threadPool,
            ingestLoadPublisher,
            () -> nodeIngestLoad,
            true,
            minSensitivityRatio,
            samplingFrequency,
            maxTimeBetweenMetricPublications,
            numProcessors
        ).start();

        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis());

        assertThat(
            publishedMetrics,
            is(equalTo(List.of(Tuple.tuple(0L, nodeIngestLoad), Tuple.tuple(maxTimeBetweenMetricPublications.millis(), nodeIngestLoad))))
        );
    }

    public void testMetricsArePublishedAlwaysWhenSensitivityIsZero() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();

        var minSensitivityRatio = 0;
        var samplingFrequency = TimeValue.timeValueSeconds(1);
        var maxTimeBetweenMetricPublications = TimeValue.timeValueSeconds(10);
        var numProcessors = randomIntBetween(2, 32);

        // Increasing load
        var indexLoadOverTime = IntStream.range(0, 32).mapToDouble(l -> l).boxed().toList();
        var currentIndexLoadSupplier = indexLoadOverTime.iterator();

        var publishedMetrics = new ArrayList<Double>();
        var ingestLoadPublisher = new IngestLoadPublisher(null, (Supplier<String>) null, null) {
            @Override
            public void publishIngestionLoad(double ingestionLoad, ActionListener<Void> listener) {
                publishedMetrics.add(ingestionLoad);
                listener.onResponse(null);
            }
        };

        new IngestLoadSampler(
            threadPool,
            ingestLoadPublisher,
            currentIndexLoadSupplier::next,
            true,
            minSensitivityRatio,
            samplingFrequency,
            maxTimeBetweenMetricPublications,
            numProcessors
        ).start();

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
        int numProcessors = 32;

        var changesAboveMinSensitivity = new AtomicInteger();
        var publishedMetrics = new ArrayList<Double>();
        DoubleSupplier ingestLoadProbe = () -> {
            // First reading
            if (publishedMetrics.isEmpty()) {
                changesAboveMinSensitivity.incrementAndGet();
                return randomIngestionLoad(numProcessors);
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

        var ingestLoadPublisher = new IngestLoadPublisher(null, (Supplier<String>) null, null) {
            @Override
            public void publishIngestionLoad(double ingestionLoad, ActionListener<Void> listener) {
                publishedMetrics.add(ingestionLoad);
                listener.onResponse(null);
            }
        };

        new IngestLoadSampler(
            threadPool,
            ingestLoadPublisher,
            ingestLoadProbe,
            true,
            minSensitivityRatio,
            samplingFrequency,
            maxTimeBetweenMetricPublications,
            numProcessors
        ).start();

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
        int numProcessors = 32;

        var ingestLoadsOverTime = IntStream.range(0, 15).mapToObj(n -> n * numProcessors * (minSensitivityRatio - 0.01)).toList();
        var readingIter = ingestLoadsOverTime.iterator();

        var publishedMetrics = new ArrayList<Double>();
        var ingestLoadPublisher = new IngestLoadPublisher(null, (Supplier<String>) null, null) {
            @Override
            public void publishIngestionLoad(double ingestionLoad, ActionListener<Void> listener) {
                publishedMetrics.add(ingestionLoad);
                listener.onResponse(null);
            }
        };

        new IngestLoadSampler(
            threadPool,
            ingestLoadPublisher,
            readingIter::next,
            true,
            minSensitivityRatio,
            samplingFrequency,
            maxTimeBetweenMetricPublications,
            numProcessors
        ).start();

        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis());

        // | time | ingest_load | ratio_diff | latest_published_ingest_load | publish |
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
        int numProcessors = 32;

        var publishedMetrics = new ArrayList<Double>();
        var significantLoadChangeCount = new AtomicInteger();

        DoubleSupplier currentIndexLoadSupplier = () -> {
            // first sample
            if (publishedMetrics.isEmpty()) {
                significantLoadChangeCount.incrementAndGet();
                return randomIngestionLoad(numProcessors);
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

        var ingestLoadPublisher = new IngestLoadPublisher(null, (Supplier<String>) null, null) {
            @Override
            public void publishIngestionLoad(double ingestionLoad, ActionListener<Void> listener) {
                publishedMetrics.add(ingestionLoad);
                listener.onResponse(null);
            }
        };

        new IngestLoadSampler(
            threadPool,
            ingestLoadPublisher,
            currentIndexLoadSupplier,
            true,
            minSensitivityRatio,
            samplingFrequency,
            maxTimeBetweenMetricPublications,
            numProcessors
        ).start();

        runFor(deterministicTaskQueue, maxTimeBetweenMetricPublications.millis());

        // Initial reading + maxTimeBetweenPublications
        assertThat(publishedMetrics.toString(), publishedMetrics, hasSize(2));
    }

    public void testSamplingStopsWhenTheServiceIsStopped() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var indexLoad = randomIngestionLoad(randomIntBetween(2, 32));

        var publishedMetrics = new ArrayList<Double>();
        var ingestLoadPublisher = new IngestLoadPublisher(null, (Supplier<String>) null, null) {
            @Override
            public void publishIngestionLoad(double ingestionLoad, ActionListener<Void> listener) {
                publishedMetrics.add(ingestionLoad);
                listener.onResponse(null);
            }
        };

        var sampler = new IngestLoadSampler(threadPool, ingestLoadPublisher, () -> indexLoad, true, Settings.EMPTY);

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

    public void testSamplingIsNotScheduledForNonIndexNodes() {
        var deterministicTaskQueue = new DeterministicTaskQueue();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var indexLoad = randomIngestionLoad(randomIntBetween(2, 32));

        var publishedMetrics = new ArrayList<Double>();
        var ingestLoadPublisher = new IngestLoadPublisher(null, (Supplier<String>) null, null) {
            @Override
            public void publishIngestionLoad(double ingestionLoad, ActionListener<Void> listener) {
                publishedMetrics.add(ingestionLoad);
                listener.onResponse(null);
            }
        };

        var sampler = new IngestLoadSampler(threadPool, ingestLoadPublisher, () -> indexLoad, false, Settings.EMPTY);

        // The sampler does not schedule any task before starting the service or publish any metrics
        assertThat(publishedMetrics, is(empty()));
        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(false));

        sampler.start();

        // The sampler is not started since the node is not an index node
        assertThat(publishedMetrics, is(empty()));
        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(false));
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

    private double randomIngestionLoad(int bound) {
        return randomDoubleBetween(0, bound, true);
    }
}
