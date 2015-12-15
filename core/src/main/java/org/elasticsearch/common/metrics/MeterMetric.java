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

package org.elasticsearch.common.metrics;

import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A meter metric which measures mean throughput and one-, five-, and
 * fifteen-minute exponentially-weighted moving average throughputs.
 *
 * <p>
 * taken from codahale metric module, replaced with LongAdder
 *
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
public class MeterMetric implements Metric {
    private static final long INTERVAL = 5; // seconds

    private final EWMA m1Rate = EWMA.oneMinuteEWMA();
    private final EWMA m5Rate = EWMA.fiveMinuteEWMA();
    private final EWMA m15Rate = EWMA.fifteenMinuteEWMA();

    private final LongAdder count = new LongAdder();
    private final long startTime = System.nanoTime();
    private final TimeUnit rateUnit;
    private final ScheduledFuture<?> future;

    public MeterMetric(ScheduledExecutorService tickThread, TimeUnit rateUnit) {
        this.rateUnit = rateUnit;
        this.future = tickThread.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                tick();
            }
        }, INTERVAL, INTERVAL, TimeUnit.SECONDS);
    }

    public TimeUnit rateUnit() {
        return rateUnit;
    }

    /**
     * Updates the moving averages.
     */
    void tick() {
        m1Rate.tick();
        m5Rate.tick();
        m15Rate.tick();
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark() {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n) {
        count.add(n);
        m1Rate.update(n);
        m5Rate.update(n);
        m15Rate.update(n);
    }

    public long count() {
        return count.sum();
    }

    public double fifteenMinuteRate() {
        return m15Rate.rate(rateUnit);
    }

    public double fiveMinuteRate() {
        return m5Rate.rate(rateUnit);
    }

    public double meanRate() {
        long count = count();
        if (count == 0) {
            return 0.0;
        } else {
            final long elapsed = (System.nanoTime() - startTime);
            return convertNsRate(count / (double) elapsed);
        }
    }

    public double oneMinuteRate() {
        return m1Rate.rate(rateUnit);
    }

    private double convertNsRate(double ratePerNs) {
        return ratePerNs * (double) rateUnit.toNanos(1);
    }

    public void stop() { FutureUtils.cancel(future);}
}
