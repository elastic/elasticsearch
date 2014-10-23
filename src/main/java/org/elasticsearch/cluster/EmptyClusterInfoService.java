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

package org.elasticsearch.cluster;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * ClusterInfoService that provides empty maps for disk usage and shard sizes
 */
public class EmptyClusterInfoService extends AbstractComponent implements ClusterInfoService {

    private final static class Holder {
        private final static EmptyClusterInfoService instance = new EmptyClusterInfoService();
    }
    private final ClusterInfo emptyClusterInfo;

    private EmptyClusterInfoService() {
        super(ImmutableSettings.EMPTY);
        emptyClusterInfo = new ClusterInfo(ImmutableMap.<String, DiskUsage>of(), ImmutableMap.<String, Long>of());
    }

    public static EmptyClusterInfoService getInstance() {
        return Holder.instance;
    }

    @Override
    public ClusterInfo getClusterInfo() {
        return emptyClusterInfo;
    }

    @Override
    public void addListener(Listener listener) {
        // no-op, no new info is ever gathered, so adding listeners is useless
    }
}
