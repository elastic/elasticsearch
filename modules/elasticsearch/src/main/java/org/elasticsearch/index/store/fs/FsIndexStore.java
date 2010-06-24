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

package org.elasticsearch.index.store.fs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;

import java.io.File;

/**
 * @author kimchy (shay.banon)
 */
public abstract class FsIndexStore extends AbstractIndexComponent implements IndexStore {

    private final File location;

    public FsIndexStore(Index index, @IndexSettings Settings indexSettings, NodeEnvironment nodeEnv) {
        super(index, indexSettings);
        this.location = new File(new File(nodeEnv.nodeFile(), "indices"), index.name());

        if (!location.exists()) {
            for (int i = 0; i < 5; i++) {
                if (location.mkdirs()) {
                    break;
                }
            }
        }
    }

    @Override public boolean persistent() {
        return true;
    }

    @Override public ByteSizeValue backingStoreTotalSpace() {
        long totalSpace = location.getTotalSpace();
        if (totalSpace == 0) {
            totalSpace = -1;
        }
        return new ByteSizeValue(totalSpace);
    }

    @Override public ByteSizeValue backingStoreFreeSpace() {
        long usableSpace = location.getUsableSpace();
        if (usableSpace == 0) {
            usableSpace = -1;
        }
        return new ByteSizeValue(usableSpace);
    }

    public File location() {
        return location;
    }

    public File shardLocation(ShardId shardId) {
        return new File(new File(location, Integer.toString(shardId.id())), "index");
    }
}
