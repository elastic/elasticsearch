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

package org.elasticsearch.test.store;

import org.apache.lucene.store.Directory;
import org.elasticsearch.cache.memory.ByteBufferCache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;

import java.io.IOException;

public class MockRamDirecorySerivce extends AbstractIndexShardComponent implements DirectoryService {

    private final MockDirectoryHelper helper;
    private final DirectoryService delegateService;

    @Inject
    public MockRamDirecorySerivce(ShardId shardId, Settings indexSettings, ByteBufferCache byteBufferCache) {
        super(shardId, indexSettings);
        helper = new MockDirectoryHelper(shardId, indexSettings, logger);
        delegateService = helper.randomRamDirecoryService(byteBufferCache);
    }

    @Override
    public Directory[] build() throws IOException {
        return helper.wrapAllInplace(delegateService.build());
    }

    @Override
    public long throttleTimeInNanos() {
        return delegateService.throttleTimeInNanos();
    }

    @Override
    public void renameFile(Directory dir, String from, String to) throws IOException {
        delegateService.renameFile(dir, from, to);
    }

    @Override
    public void fullDelete(Directory dir) throws IOException {
        delegateService.fullDelete(dir);
    }
}
