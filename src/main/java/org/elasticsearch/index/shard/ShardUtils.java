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

package org.elasticsearch.index.shard;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.store.Store;

/**
 */
public class ShardUtils {

    /**
     * Tries to extract the shard id from a reader if possible, when its not possible,
     * will return null. This method requires the reader to be a {@link SegmentReader}
     * and the directory backing it to be {@link org.elasticsearch.index.store.Store.StoreDirectory}.
     * This will be the case in almost all cases, except for percolator currently.
     */
    @Nullable
    public static ShardId extractShardId(IndexReader reader) {
        if (reader instanceof SegmentReader) {
            SegmentReader sReader = (SegmentReader) reader;
            if (sReader.directory() instanceof Store.StoreDirectory) {
                return ((Store.StoreDirectory) sReader.directory()).shardId();
            }
        }
        return null;
    }
}
