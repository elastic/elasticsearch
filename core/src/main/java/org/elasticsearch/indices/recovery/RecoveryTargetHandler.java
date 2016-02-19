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
package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;


public interface RecoveryTargetHandler {

    /**
     * Prepares the tranget to receive translog operations, after all file have been copied
     *
     * @param totalTranslogOps total translog operations expected to be sent
     */
    void prepareForTranslogOperations(int totalTranslogOps) throws IOException;

    /**
     * The finalize request clears unreferenced translog files, refreshes the engine now that
     * new segments are available, and enables garbage collection of
     * tombstone files. The shard is also moved to the POST_RECOVERY phase during this time
     **/
    void finalizeRecovery();

    /**
     * Index a set of translog operations on the target
     * @param operations operations to index
     * @param totalTranslogOps current number of total operations expected to be indexed
     */
    void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps);

    /**
     * Notifies the target of the files it is going to receive
     */
    void receiveFileInfo(List<String> phase1FileNames,
                         List<Long> phase1FileSizes,
                         List<String> phase1ExistingFileNames,
                         List<Long> phase1ExistingFileSizes,
                         int totalTranslogOps);

    /**
     * After all source files has been sent over, this command is sent to the target so it can clean any local
     * files that are not part of the source store
     * @param totalTranslogOps an update number of translog operations that will be replayed later on
     * @param sourceMetaData meta data of the source store
     */
    void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException;

    /** writes a partial file chunk to the target store */
    void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content,
                        boolean lastChunk, int totalTranslogOps) throws IOException;

}
