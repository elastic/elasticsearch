/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.timer;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.timer.HashedWheelTimer;
import org.elasticsearch.common.timer.Timeout;
import org.elasticsearch.common.timer.Timer;
import org.elasticsearch.common.timer.TimerTask;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.*;
import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TimerService extends AbstractComponent {

    private final ThreadPool threadPool;

    private final TimeEstimator timeEstimator;

    private final ScheduledFuture timeEstimatorFuture;

    private final Timer timer;

    private final TimeValue tickDuration;

    private final int ticksPerWheel;

    public TimerService(ThreadPool threadPool) {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, threadPool);
    }

    @Inject public TimerService(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;

        this.timeEstimator = new TimeEstimator();
        this.timeEstimatorFuture = threadPool.scheduleWithFixedDelay(timeEstimator, 50, 50, TimeUnit.MILLISECONDS);

        this.tickDuration = componentSettings.getAsTime("tick_duration", timeValueMillis(100));
        this.ticksPerWheel = componentSettings.getAsInt("ticks_per_wheel", 1024);

        this.timer = new HashedWheelTimer(logger, daemonThreadFactory(settings, "timer"), tickDuration.millis(), TimeUnit.MILLISECONDS, ticksPerWheel);
    }

    public void close() {
        timeEstimatorFuture.cancel(true);
        timer.stop();
    }

    public long estimatedTimeInMillis() {
        return timeEstimator.time();
    }

    public Timeout newTimeout(TimerTask task, TimeValue delay) {
        return newTimeout(task, delay.nanos(), TimeUnit.NANOSECONDS);
    }

    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        return timer.newTimeout(task, delay, unit);
    }

    private static class TimeEstimator implements Runnable {

        private long time = System.currentTimeMillis();

        @Override public void run() {
            this.time = System.currentTimeMillis();
        }

        public long time() {
            return this.time;
        }
    }
}
