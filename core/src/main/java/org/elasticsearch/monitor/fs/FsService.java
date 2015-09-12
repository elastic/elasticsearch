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

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;

import java.io.IOException;

/**
 */
public class FsService extends AbstractComponent {

    private final FsProbe probe;

    private final SingleObjectCache<FsInfo> fsStatsCache;

    @Inject
    public FsService(Settings settings, FsProbe probe) throws IOException {
        super(settings);
        this.probe = probe;
        TimeValue refreshInterval = settings.getAsTime("monitor.fs.refresh_interval", TimeValue.timeValueSeconds(1));
        fsStatsCache = new FsInfoCache(refreshInterval, probe.stats());
        logger.debug("Using probe [{}] with refresh_interval [{}]", probe, refreshInterval);
    }

    public FsInfo stats() {
        return fsStatsCache.getOrRefresh();
    }

    private class FsInfoCache extends SingleObjectCache<FsInfo> {
        public FsInfoCache(TimeValue interval, FsInfo initValue) {
            super(interval, initValue);
        }

        @Override
        protected FsInfo refresh() {
            try {
                return probe.stats();
            } catch (IOException ex) {
                logger.warn("Failed to fetch fs stats - returning empty instance");
                return new FsInfo();
            }
        }
    }

}
