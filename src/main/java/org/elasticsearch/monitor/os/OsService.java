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

package org.elasticsearch.monitor.os;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class OsService extends AbstractComponent {

    private final OsProbe probe;

    private final OsInfo info;

    private final TimeValue refreshInterval;

    private OsStats cachedStats;

    @Inject
    public OsService(Settings settings, OsProbe probe) {
        super(settings);
        this.probe = probe;

        this.refreshInterval = componentSettings.getAsTime("refresh_interval", TimeValue.timeValueSeconds(1));

        this.info = probe.osInfo();
        this.info.refreshInterval = refreshInterval.millis();
        this.info.availableProcessors = Runtime.getRuntime().availableProcessors();
        this.cachedStats = probe.osStats();

        logger.debug("Using probe [{}] with refresh_interval [{}]", probe, refreshInterval);
    }

    public OsInfo info() {
        return this.info;
    }

    public synchronized OsStats stats() {
        if ((System.currentTimeMillis() - cachedStats.timestamp()) > refreshInterval.millis()) {
            cachedStats = probe.osStats();
        }
        return cachedStats;
    }
}
