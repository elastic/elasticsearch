/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class RecoveryStatus {

    public static enum Stage {
        INIT,
        INDEX,
        TRANSLOG,
        FINALIZE,
        DONE
    }

    volatile Thread recoveryThread;
    volatile boolean canceled;
    volatile boolean sentCanceledToSource;

    ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();
    ConcurrentMap<String, String> checksums = ConcurrentCollections.newConcurrentMap();

    final long startTime = System.currentTimeMillis();
    long time;
    List<String> phase1FileNames;
    List<Long> phase1FileSizes;
    List<String> phase1ExistingFileNames;
    List<Long> phase1ExistingFileSizes;
    long phase1TotalSize;
    long phase1ExistingTotalSize;

    volatile Stage stage = Stage.INIT;
    volatile long currentTranslogOperations = 0;
    AtomicLong currentFilesSize = new AtomicLong();

    public long startTime() {
        return startTime;
    }

    public long time() {
        return this.time;
    }

    public long phase1TotalSize() {
        return phase1TotalSize;
    }

    public long phase1ExistingTotalSize() {
        return phase1ExistingTotalSize;
    }

    public Stage stage() {
        return stage;
    }

    public long currentTranslogOperations() {
        return currentTranslogOperations;
    }

    public long currentFilesSize() {
        return currentFilesSize.get();
    }
}
