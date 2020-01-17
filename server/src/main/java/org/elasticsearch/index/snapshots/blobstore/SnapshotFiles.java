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

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains a list of files participating in a snapshot
 */
public class SnapshotFiles {

    private final String snapshot;

    private final List<FileInfo> indexFiles;

    private Map<String, FileInfo> physicalFiles = null;

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return snapshot;
    }

    /**
     * @param snapshot   snapshot name
     * @param indexFiles index files
     */
    public SnapshotFiles(String snapshot, List<FileInfo> indexFiles ) {
        this.snapshot = snapshot;
        this.indexFiles = indexFiles;
    }

    /**
     * Returns a list of file in the snapshot
     */
    public List<FileInfo> indexFiles() {
        return indexFiles;
    }

    /**
     * Returns true if this snapshot contains a file with a given original name
     *
     * @param physicalName original file name
     * @return true if the file was found, false otherwise
     */
    public boolean containPhysicalIndexFile(String physicalName) {
        return findPhysicalIndexFile(physicalName) != null;
    }

    /**
     * Returns information about a physical file with the given name
     * @param physicalName the original file name
     * @return information about this file
     */
    private FileInfo findPhysicalIndexFile(String physicalName) {
        if (physicalFiles == null) {
            Map<String, FileInfo> files = new HashMap<>();
            for(FileInfo fileInfo : indexFiles) {
                files.put(fileInfo.physicalName(), fileInfo);
            }
            this.physicalFiles = files;
        }
        return physicalFiles.get(physicalName);
    }

}
