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

package org.elasticsearch.monitor.fs;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.env.NodeEnvironment;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class FsService extends AbstractLifecycleComponent<FsService> {

    private final FsProbe probe;
    private final TimeValue refreshInterval;
    private final BiFunction<Runnable, TimeValue, ScheduledFuture> scheduler;
    private final AtomicReference<FsInfo> ref = new AtomicReference<>();
    private final Runnable probeRunner;
    private ScheduledFuture scheduledFuture;

    public final static Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("monitor.fs.refresh_interval", TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(1),
            Property.NodeScope);

    public FsService(
            final Settings settings,
            final NodeEnvironment nodeEnvironment,
            final BiFunction<Runnable, TimeValue, ScheduledFuture> scheduler) throws IOException {
        super(settings);
        this.probe = new FsProbe(settings, nodeEnvironment);
        refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.scheduler = scheduler;
        probeRunner = () -> {
            try {
                final FsInfo fsInfo = probe.stats(ref.get());
                ref.set(fsInfo);
            } catch (IOException e) {
            }
        };
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    public FsInfo stats() {
        return ref.get();
    }

    @Override
    protected void doStart() {
        // force the probe to run in case stats is called before the
        // first scheduled run occurs
        probeRunner.run();
        // now schedule the probe to run periodically
        scheduledFuture = scheduler.apply(probeRunner, refreshInterval);
    }

    @Override
    protected void doStop() {
        FutureUtils.cancel(scheduledFuture);
    }

    @Override
    protected void doClose() {
        FutureUtils.cancel(scheduledFuture);
    }

}
