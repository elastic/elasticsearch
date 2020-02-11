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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicReference;

public class RecordJFR {

    public synchronized static void record(String name, Histogram histogram) {
        HistogramEvent event = new HistogramEvent();
        if (event.isEnabled() == false) {
            return;
        }

        event.begin();
        event._10 = histogram.getValueAtPercentile(0.10);
        event._50 = histogram.getValueAtPercentile(0.50);
        event._90 = histogram.getValueAtPercentile(0.90);
        event._99 = histogram.getValueAtPercentile(0.99);
        event._99_9 = histogram.getValueAtPercentile(0.999);
        event.max = histogram.getMaxValue();
        event.mean = histogram.getMean();
        event.total = histogram.getTotalCount();
        event.name = name;
        event.end();
        event.commit();
    }

    public static void scheduleHistogramSample(ThreadPool threadPool, AtomicReference<Recorder> recorder) {
        AtomicReference<Histogram> toReuse = new AtomicReference<>(null);

        threadPool.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                synchronized (this) {
                    Histogram histogramToRecycle = toReuse.get();
                    if (histogramToRecycle != null) {
                        histogramToRecycle.reset();
                    }
                    Histogram intervalHistogram = recorder.get().getIntervalHistogram(histogramToRecycle);
                    toReuse.set(intervalHistogram);
                    RecordJFR.record("TransportBulkAction", intervalHistogram);
                }
            }
        }, TimeValue.timeValueSeconds(10), ThreadPool.Names.GENERIC);
    }

    @Name(HistogramEvent.NAME)
    @Label("Histogram")
    @Category("Elasticsearch")
    @StackTrace(false)
    public static class HistogramEvent extends Event {

        static final String NAME = "org.elasticsearch.jfr.HistogramEvent";

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

        @Label("Max")
        public long max;

        @Label("Mean")
        public double mean;

        @Label("Total")
        public long total;

    }
}
