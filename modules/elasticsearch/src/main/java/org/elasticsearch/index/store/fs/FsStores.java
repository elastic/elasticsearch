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

import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class FsStores {

    public static final String DEFAULT_INDICES_LOCATION = "indices";

    public static synchronized File createStoreFilePath(File basePath, String localNodeId, ShardId shardId) throws IOException {
        // TODO we need to clean the nodeId from invalid folder characters
        File f = new File(new File(basePath, DEFAULT_INDICES_LOCATION), localNodeId);
        f = new File(f, shardId.index().name());
        f = new File(f, Integer.toString(shardId.id()));

        if (f.exists() && f.isDirectory()) {
            return f;
        }
        boolean result = false;
        for (int i = 0; i < 5; i++) {
            result = f.mkdirs();
            if (result) {
                break;
            }
        }
        if (!result) {
            if (f.exists() && f.isDirectory()) {
                return f;
            }
            throw new IOException("Failed to create directories for [" + f + "]");
        }
        return f;
    }
}
