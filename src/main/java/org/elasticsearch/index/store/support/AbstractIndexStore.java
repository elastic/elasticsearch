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

package org.elasticsearch.index.store.support;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;

import java.io.IOException;

/**
 *
 */
public abstract class AbstractIndexStore extends AbstractIndexComponent implements IndexStore {

    protected final IndexService indexService;

    protected AbstractIndexStore(Index index, @IndexSettings Settings indexSettings, IndexService indexService) {
        super(index, indexSettings);
        this.indexService = indexService;
    }

    @Override
    public boolean canDeleteUnallocated(ShardId shardId) {
        return false;
    }

    @Override
    public void deleteUnallocated(ShardId shardId) throws IOException {
        // do nothing here...
    }
}
