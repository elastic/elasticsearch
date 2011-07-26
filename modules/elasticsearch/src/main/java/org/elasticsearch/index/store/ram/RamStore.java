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

package org.elasticsearch.index.store.ram;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.RAMFile;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.support.AbstractStore;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class RamStore extends AbstractStore {

    private final CustomRAMDirectory ramDirectory;

    private final Directory directory;

    @Inject public RamStore(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore) throws IOException {
        super(shardId, indexSettings, indexStore);
        this.ramDirectory = new CustomRAMDirectory();
        this.directory = wrapDirectory(ramDirectory);
        logger.debug("Using [ram] Store");
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

    @Override protected void doRenameFile(String from, String to) throws IOException {
        ramDirectory.renameTo(from, to);
    }

    static class CustomRAMDirectory extends RAMDirectory {

        public synchronized void renameTo(String from, String to) throws IOException {
            RAMFile fromFile = fileMap.get(from);
            if (fromFile == null)
                throw new FileNotFoundException(from);
            RAMFile toFile = fileMap.get(to);
            if (toFile != null) {
                sizeInBytes.addAndGet(-fileLength(from));
                fileMap.remove(from);
            }
            fileMap.put(to, fromFile);
        }
    }
}
