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

package org.elasticsearch.index.store.memory;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.support.AbstractStore;

/**
 * @author kimchy (Shay Banon)
 */
public class HeapStore extends AbstractStore<HeapDirectory> {

    private final ByteSizeValue bufferSize;

    private final ByteSizeValue cacheSize;

    private final boolean warmCache;

    private HeapDirectory directory;

    @Inject public HeapStore(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);

        this.bufferSize = componentSettings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB));
        this.cacheSize = componentSettings.getAsBytesSize("cache_size", new ByteSizeValue(20, ByteSizeUnit.MB));
        this.warmCache = componentSettings.getAsBoolean("warm_cache", true);

        this.directory = new HeapDirectory(bufferSize, cacheSize, warmCache);
        logger.debug("Using [heap] Store with buffer_size[{}], cache_size[{}], warm_cache[{}]", directory.bufferSize(), directory.cacheSize(), warmCache);
    }

    @Override public HeapDirectory directory() {
        return directory;
    }

    /**
     * Its better to not use the compound format when using the Ram store.
     */
    @Override public boolean suggestUseCompoundFile() {
        return false;
    }
}