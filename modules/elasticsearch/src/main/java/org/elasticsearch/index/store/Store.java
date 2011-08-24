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

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.IndexShardComponent;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public interface Store extends IndexShardComponent {

    /**
     * The Lucene {@link Directory} this store is using.
     */
    Directory directory();

    IndexOutput createOutputWithNoChecksum(String name) throws IOException;

    void writeChecksum(String name, String checksum) throws IOException;

    void writeChecksums(Map<String, String> checksums) throws IOException;

    StoreFileMetaData metaData(String name) throws IOException;

    ImmutableMap<String, StoreFileMetaData> list() throws IOException;

    /**
     * Just deletes the content of the store.
     */
    void deleteContent() throws IOException;

    /**
     * Renames, note, might not be atomic, and can fail "in the middle".
     */
    void renameFile(String from, String to) throws IOException;

    /**
     * Deletes the store completely. For example, in FS ones, also deletes the parent
     * directory.
     */
    void fullDelete() throws IOException;

    StoreStats stats() throws IOException;

    /**
     * The estimated size this store is using.
     */
    ByteSizeValue estimateSize() throws IOException;

    /**
     * The store can suggest the best setting for compound file the
     * {@link org.apache.lucene.index.MergePolicy} will use.
     */
    boolean suggestUseCompoundFile();

    /**
     * Close the store.
     */
    void close() throws IOException;
}
