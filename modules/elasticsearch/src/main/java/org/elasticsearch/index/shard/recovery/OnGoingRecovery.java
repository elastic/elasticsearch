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

package org.elasticsearch.index.shard.recovery;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (shay.banon)
 */
public class OnGoingRecovery {

    public static enum Stage {
        INIT,
        FILES,
        TRANSLOG,
        FINALIZE
    }

    ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();

    List<String> phase1FileNames;
    List<Long> phase1FileSizes;
    List<String> phase1ExistingFileNames;
    List<Long> phase1ExistingFileSizes;
    long phase1TotalSize;
    long phase1ExistingTotalSize;

    volatile Stage stage = Stage.INIT;
    volatile long currentTranslogOperations = 0;
}
