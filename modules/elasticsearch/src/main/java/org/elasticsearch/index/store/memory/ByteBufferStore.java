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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.bytebuffer.ByteBufferDirectory;
import org.elasticsearch.cache.memory.ByteBufferCache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.support.AbstractStore;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class ByteBufferStore extends AbstractStore {

    private final Directory directory;

    @Inject public ByteBufferStore(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore, ByteBufferCache byteBufferCache) throws IOException {
        super(shardId, indexSettings, indexStore);

        this.directory = wrapDirectory(new ByteBufferDirectory(byteBufferCache));
        logger.debug("Using [byte_buffer] store");
    }

    @Override public Directory directory() {
        return directory;
    }

    /**
     * Its better to not use the compound format when using the Ram store.
     */
    @Override public boolean suggestUseCompoundFile() {
        return false;
    }
}
