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

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;


public interface RecoveryTargetHandler {
    void prepareForTranslogOperations(int totalTranslogOps) throws IOException;

    /**
     * The finalize request clears unreferenced translog files, refreshes the engine now that
     * new segments are available, and enables garbage collection of
     * tombstone files. The shard is also moved to the POST_RECOVERY phase during this time
     **/
    void finalizeRecovery();

    void indexTranslogOperations(Iterable<Translog.Operation> operations, int totalTranslogOps);

    void receiveFileInfo(List<String> phase1FileNames,
                         List<Long> phase1FileSizes,
                         List<String> phase1ExistingFileNames,
                         List<Long> phase1ExistingFileSizes,
                         int totalTranslogOps);

    void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException;

    void writeFileChunk(String name, long position, StoreFileMetaData metadata, BytesReference content,
                        long length, boolean lastChunk, int totalTranslogOps,
                        long sourceThrottleTimeInNanos, @Nullable RateLimiter rateLimiter) throws IOException;

    class RetryTranslogOpsException extends Exception {

        public RetryTranslogOpsException(Throwable cause) {
            super(cause);
        }
    }
}
