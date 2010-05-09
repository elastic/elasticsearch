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

import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.support.AbstractStore;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (shay.banon)
 */
public class ByteBufferStore extends AbstractStore<ByteBufferDirectory> {

    private final SizeValue bufferSize;

    private final SizeValue cacheSize;

    private final boolean direct;

    private final boolean warmCache;

    private final ByteBufferDirectory directory;

    @Inject public ByteBufferStore(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);

        this.bufferSize = componentSettings.getAsSize("buffer_size", new SizeValue(100, SizeUnit.KB));
        this.cacheSize = componentSettings.getAsSize("cache_size", new SizeValue(20, SizeUnit.MB));
        this.direct = componentSettings.getAsBoolean("direct", true);
        this.warmCache = componentSettings.getAsBoolean("warm_cache", true);
        this.directory = new ByteBufferDirectory((int) bufferSize.bytes(), (int) cacheSize.bytes(), direct, warmCache);
        logger.debug("Using [byte_buffer] store with buffer_size[{}], cache_size[{}], direct[{}], warm_cache[{}]",
                new Object[]{bufferSize, cacheSize, directory.isDirect(), warmCache});
    }

    @Override public ByteBufferDirectory directory() {
        return directory;
    }

    /**
     * Its better to not use the compound format when using the Ram store.
     */
    @Override public boolean suggestUseCompoundFile() {
        return false;
    }
}
