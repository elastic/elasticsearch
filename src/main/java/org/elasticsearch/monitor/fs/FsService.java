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

package org.elasticsearch.monitor.fs;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 */
public class FsService extends AbstractComponent {

    private final FsProbe probe;

    private final TimeValue refreshInterval;

    private FsStats cachedStats;

    @Inject
    public FsService(Settings settings, FsProbe probe) {
        super(settings);
        this.probe = probe;
        this.cachedStats = probe.stats();

        this.refreshInterval = componentSettings.getAsTime("refresh_interval", TimeValue.timeValueSeconds(1));

        logger.debug("Using probe [{}] with refresh_interval [{}]", probe, refreshInterval);
    }

    public synchronized FsStats stats() {
        if ((System.currentTimeMillis() - cachedStats.getTimestamp()) > refreshInterval.millis()) {
            cachedStats = probe.stats();
        }
        return cachedStats;
    }

}
