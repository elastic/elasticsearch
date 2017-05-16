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

package org.elasticsearch.legacy.test.store;

import org.elasticsearch.legacy.common.inject.Inject;

import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.unit.ByteSizeValue;
import org.elasticsearch.legacy.index.Index;
import org.elasticsearch.legacy.index.service.IndexService;
import org.elasticsearch.legacy.index.store.DirectoryService;
import org.elasticsearch.legacy.index.store.support.AbstractIndexStore;
import org.elasticsearch.legacy.indices.store.IndicesStore;
import org.elasticsearch.legacy.monitor.jvm.JvmInfo;
import org.elasticsearch.legacy.monitor.jvm.JvmStats;

public class MockRamIndexStore extends AbstractIndexStore{

    @Inject
    public MockRamIndexStore(Index index, Settings indexSettings, IndexService indexService, IndicesStore indicesStore) {
        super(index, indexSettings, indexService, indicesStore);
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public Class<? extends DirectoryService> shardDirectory() {
        return MockRamDirectoryService.class;
    }

    @Override
    public ByteSizeValue backingStoreTotalSpace() {
        return JvmInfo.jvmInfo().getMem().heapMax();
    }

    @Override
    public ByteSizeValue backingStoreFreeSpace() {
        return JvmStats.jvmStats().getMem().heapUsed();
    }

}
