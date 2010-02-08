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
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.IndexShardLifecycle;
import org.elasticsearch.util.SizeValue;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
@IndexShardLifecycle
public interface Store<T extends Directory> extends IndexShardComponent {

    /**
     * The Lucene {@link Directory} this store is using.
     */
    T directory();

    /**
     * Just deletes the content of the store.
     */
    void deleteContent() throws IOException;

    /**
     * Deletes the store completely. For example, in FS ones, also deletes the parent
     * directory.
     */
    void fullDelete() throws IOException;

    /**
     * The estimated size this store is using.
     */
    SizeValue estimateSize() throws IOException;

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
