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

package org.elasticsearch.cache;

import org.elasticsearch.cache.memory.ByteBufferCache;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public class NodeCache extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;

    private final ByteBufferCache byteBufferCache;

    @Inject
    public NodeCache(Settings settings, ByteBufferCache byteBufferCache, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        this.byteBufferCache = byteBufferCache;
        clusterService.add(this);
    }

    public void close() {
        clusterService.remove(this);
        byteBufferCache.close();
    }

    public ByteBufferCache byteBuffer() {
        return byteBufferCache;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
    }
}
