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

package org.elasticsearch.index.snapshots.blobstore;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.util.Iterator;
import java.util.List;

/**
 * Contains information about all snapshot for the given shard in repository
 * <p/>
 * This class is used to find files that were already snapshoted and clear out files that no longer referenced by any
 * snapshots
 */
public class BlobStoreIndexShardSnapshots implements Iterable<BlobStoreIndexShardSnapshot> {
    private final ImmutableList<BlobStoreIndexShardSnapshot> shardSnapshots;

    public BlobStoreIndexShardSnapshots(List<BlobStoreIndexShardSnapshot> shardSnapshots) {
        this.shardSnapshots = ImmutableList.copyOf(shardSnapshots);
    }

    /**
     * Returns list of snapshots
     *
     * @return list of snapshots
     */
    public ImmutableList<BlobStoreIndexShardSnapshot> snapshots() {
        return this.shardSnapshots;
    }

    /**
     * Finds reference to a snapshotted file by its original name
     *
     * @param physicalName original name
     * @return file info or null if file is not present in any of snapshots
     */
    public FileInfo findPhysicalIndexFile(String physicalName) {
        for (BlobStoreIndexShardSnapshot snapshot : shardSnapshots) {
            FileInfo fileInfo = snapshot.findPhysicalIndexFile(physicalName);
            if (fileInfo != null) {
                return fileInfo;
            }
        }
        return null;
    }

    /**
     * Finds reference to a snapshotted file by its snapshot name
     *
     * @param name file name
     * @return file info or null if file is not present in any of snapshots
     */
    public FileInfo findNameFile(String name) {
        for (BlobStoreIndexShardSnapshot snapshot : shardSnapshots) {
            FileInfo fileInfo = snapshot.findNameFile(name);
            if (fileInfo != null) {
                return fileInfo;
            }
        }
        return null;
    }

    @Override
    public Iterator<BlobStoreIndexShardSnapshot> iterator() {
        return shardSnapshots.iterator();
    }
}
