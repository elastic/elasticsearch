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

package org.elasticsearch.index.store.fs;

import com.google.common.collect.Sets;
import org.apache.lucene.store.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 */
public class DefaultFsDirectoryService extends FsDirectoryService {
    /*
     * We are mmapping docvalues as well as term dictionaries, all other files are served through NIOFS
     * this provides good random access performance while not creating unnecessary mmaps for files like stored
     * fields etc.
     */
    private static final Set<String> PRIMARY_EXTENSIONS = Collections.unmodifiableSet(Sets.newHashSet("dvd", "tim"));

    @Inject
    public DefaultFsDirectoryService(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore) {
        super(shardId, indexSettings, indexStore);
    }

    @Override
    protected Directory newFSDirectory(File location, LockFactory lockFactory) throws IOException {
        return new FileSwitchDirectory(PRIMARY_EXTENSIONS, new MMapDirectory(location, lockFactory), new NIOFSDirectory(location, lockFactory), true);
    }
}
