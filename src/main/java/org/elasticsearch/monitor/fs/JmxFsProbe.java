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
import org.elasticsearch.env.NodeEnvironment;

import java.io.File;

/**
 */
public class JmxFsProbe extends AbstractComponent implements FsProbe {

    private final NodeEnvironment nodeEnv;

    @Inject
    public JmxFsProbe(Settings settings, NodeEnvironment nodeEnv) {
        super(settings);
        this.nodeEnv = nodeEnv;
    }

    @Override
    public FsStats stats() {
        if (!nodeEnv.hasNodeFile()) {
            return new FsStats(System.currentTimeMillis(), new FsStats.Info[0]);
        }
        File[] dataLocations = nodeEnv.nodeDataLocations();
        FsStats.Info[] infos = new FsStats.Info[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            File dataLocation = dataLocations[i];
            FsStats.Info info = new FsStats.Info();
            info.path = dataLocation.getAbsolutePath();
            info.total = dataLocation.getTotalSpace();
            info.free = dataLocation.getFreeSpace();
            info.available = dataLocation.getUsableSpace();
            infos[i] = info;
        }
        return new FsStats(System.currentTimeMillis(), infos);
    }
}
