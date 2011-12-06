/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.monitor.process;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class ProcessService extends AbstractComponent {

    private final ProcessProbe probe;

    private final ProcessInfo info;

    private final TimeValue refreshInterval;

    private ProcessStats cachedStats;

    @Inject
    public ProcessService(Settings settings, ProcessProbe probe) {
        super(settings);
        this.probe = probe;

        this.refreshInterval = componentSettings.getAsTime("refresh_interval", TimeValue.timeValueSeconds(1));

        this.info = probe.processInfo();
        this.info.refreshInterval = refreshInterval.millis();
        this.cachedStats = probe.processStats();

        logger.debug("Using probe [{}] with refresh_interval [{}]", probe, refreshInterval);
    }

    public ProcessInfo info() {
        return this.info;
    }

    public synchronized ProcessStats stats() {
        if ((System.currentTimeMillis() - cachedStats.timestamp()) > refreshInterval.millis()) {
            cachedStats = probe.processStats();
        }
        return cachedStats;
    }
}
