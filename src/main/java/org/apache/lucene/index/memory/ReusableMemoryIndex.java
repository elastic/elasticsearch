package org.apache.lucene.index.memory;

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

/**
 * This class overwrites {@link MemoryIndex} to make the reuse constructor 
 * visible. 
 */
public final class ReusableMemoryIndex extends MemoryIndex {
    private final long maxReuseBytes;
    public ReusableMemoryIndex(boolean storeOffsets, long maxReusedBytes) {
        super(storeOffsets, maxReusedBytes);
        this.maxReuseBytes = maxReusedBytes;
    }
    
    public long getMaxReuseBytes() {
        return maxReuseBytes;
    }

}
