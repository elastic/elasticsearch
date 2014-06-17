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

package org.elasticsearch.index.store.ram;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.RAMFile;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.DirectoryUtils;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 */
public final class RamDirectoryService extends AbstractIndexShardComponent implements DirectoryService {

    @Inject
    public RamDirectoryService(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    @Override
    public long throttleTimeInNanos() {
        return 0;
    }

    @Override
    public Directory[] build() {
        return new Directory[]{new CustomRAMDirectory()};
    }

    @Override
    public void renameFile(Directory dir, String from, String to) throws IOException {
        CustomRAMDirectory leaf = DirectoryUtils.getLeaf(dir, CustomRAMDirectory.class);
        assert leaf != null;
        leaf.renameTo(from, to);
    }

    @Override
    public void fullDelete(Directory dir) {
    }

    static class CustomRAMDirectory extends RAMDirectory {

        public synchronized void renameTo(String from, String to) throws IOException {
            RAMFile fromFile = fileMap.get(from);
            if (fromFile == null)
                throw new FileNotFoundException(from);
            RAMFile toFile = fileMap.get(to);
            if (toFile != null) {
                sizeInBytes.addAndGet(-fileLength(to));
            }
            fileMap.put(to, fromFile);
            fileMap.remove(from);
        }

        @Override
        public String toString() {
            return "ram";
        }
    }
}
