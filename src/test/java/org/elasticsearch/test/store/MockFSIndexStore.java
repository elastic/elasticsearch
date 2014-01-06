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

package org.elasticsearch.test.store;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.fs.FsIndexStore;
import org.elasticsearch.indices.store.IndicesStore;

public class MockFSIndexStore extends FsIndexStore {

    @Inject
    public MockFSIndexStore(Index index, Settings indexSettings, IndexService indexService, IndicesStore indicesStore, NodeEnvironment nodeEnv) {
        super(index, indexSettings, indexService, indicesStore, nodeEnv);
    }

    @Override
    public Class<? extends DirectoryService> shardDirectory() {
        return MockFSDirectoryService.class;
    }

}
