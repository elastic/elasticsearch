/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.bloom.none;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.bloom.BloomCache;
import org.elasticsearch.index.settings.IndexSettings;

/**
 * @author kimchy (shay.banon)
 */
public class NoneBloomCache extends AbstractIndexComponent implements BloomCache {

    public NoneBloomCache(Index index) {
        super(index, ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    @Inject public NoneBloomCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public BloomFilter filter(IndexReader reader, String fieldName, boolean asyncLoad) {
        return BloomFilter.NONE;
    }

    @Override public void clear() {
    }

    @Override public void clear(IndexReader reader) {
    }

    @Override public long sizeInBytes() {
        return 0;
    }

    @Override public long sizeInBytes(String fieldName) {
        return 0;
    }

    @Override public void close() throws ElasticSearchException {
    }
}