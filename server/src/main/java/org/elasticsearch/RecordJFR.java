/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch;

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.HdrHistogram.SynchronizedHistogram;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicReference;

public class RecordJFR {

    public static synchronized void recordHistogram(String name, Histogram histogram, HistogramEvent event) {
        if (event.isEnabled() == false) {
            return;
        }

        event.begin();
        event._10 = histogram.getValueAtPercentile(10.0);
        event._50 = histogram.getValueAtPercentile(50.0);
        event._90 = histogram.getValueAtPercentile(90.0);
        event._99 = histogram.getValueAtPercentile(99.0);
        event._99_9 = histogram.getValueAtPercentile(99.9);
        event._99_99 = histogram.getValueAtPercentile(99.99);
        event._99_999 = histogram.getValueAtPercentile(99.999);
        event.max = histogram.getMaxValue();
        event.mean = histogram.getMean();
        event.total = histogram.getTotalCount();
        event.name = name;
        event.end();
        event.commit();
    }

    public static synchronized void recordMeanMetric(String name, MeanMetric meanMetric) {
        MeanMetricEvent event = new MeanMetricEvent();
        if (event.isEnabled() == false) {
            return;
        }

        event.begin();
        event.mean = meanMetric.mean();
        event.sum = meanMetric.sum();
        event.counter = meanMetric.count();
        event.name = name;
        event.end();
        event.commit();
    }

    public static void scheduleMeanSample(String name, ThreadPool threadPool, AtomicReference<MeanMetric> meanMetric) {
        threadPool.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                synchronized (RecordJFR.class) {
                    MeanMetric meanMetric1 = meanMetric.getAndSet(new MeanMetric());
                    RecordJFR.recordMeanMetric(name, meanMetric1);
                }
            }
        }, TimeValue.timeValueSeconds(10), ThreadPool.Names.GENERIC);
    }

    public static void scheduleHistogramSample(String name, ThreadPool threadPool, AtomicReference<Recorder> recorder) {
        Histogram initialHistogram = recorder.get().getIntervalHistogram(null);
        SynchronizedHistogram totalHistogram = new SynchronizedHistogram(initialHistogram.getLowestDiscernibleValue(),
            initialHistogram.getHighestTrackableValue(), initialHistogram.getNumberOfSignificantValueDigits());
        totalHistogram.add(initialHistogram);
        AtomicReference<Histogram> toReuse = new AtomicReference<>(initialHistogram);

        threadPool.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                synchronized (RecordJFR.class) {
                    Histogram histogramToRecycle = toReuse.get();
                    histogramToRecycle.reset();

                    Histogram intervalHistogram = recorder.get().getIntervalHistogram(histogramToRecycle);
                    toReuse.set(intervalHistogram);
                    totalHistogram.add(intervalHistogram);
                    RecordJFR.recordHistogram(name, intervalHistogram, new IntervalHistogramEvent());
                    RecordJFR.recordHistogram(name, totalHistogram, new TotalHistogramEvent());
                }
            }
        }, TimeValue.timeValueSeconds(10), ThreadPool.Names.GENERIC);
    }

    @StackTrace(false)
    public static class HistogramEvent extends Event {

        @Label("Name")
        public String name;

        @Label("10%")
        public long _10;

        @Label("50%")
        public long _50;

        @Label("90%")
        public long _90;

        @Label("99%")
        public long _99;

        @Label("99.9%")
        public long _99_9;

        @Label("99.99%")
        public long _99_99;

        @Label("99.999%")
        public long _99_999;

        @Label("Max")
        public long max;

        @Label("Mean")
        public double mean;

        @Label("Total")
        public long total;

    }

    @Name(IntervalHistogramEvent.NAME)
    @Label("Interval Histogram")
    @Category("Elasticsearch")
    public static class IntervalHistogramEvent extends HistogramEvent {

        static final String NAME = "org.elasticsearch.jfr.IntervalHistogramEvent";

    }

    @Name(TotalHistogramEvent.NAME)
    @Label("Total Histogram")
    @Category("Elasticsearch")
    @StackTrace(false)
    public static class TotalHistogramEvent extends HistogramEvent {

        static final String NAME = "org.elasticsearch.jfr.TotalHistogramEvent";

    }

    @Name(MeanMetricEvent.NAME)
    @Label("Mean Metric")
    @Category("Elasticsearch")
    @StackTrace(false)
    public static class MeanMetricEvent extends Event {

        static final String NAME = "org.elasticsearch.jfr.MeanMetricEvent";

        @Label("Name")
        public String name;

        @Label("Mean")
        public double mean;

        @Label("Counter")
        public long counter;

        @Label("Sum")
        public long sum;

    }
}
