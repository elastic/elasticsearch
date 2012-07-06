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

package org.elasticsearch.index.cache.id.simple;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.cache.id.IdReaderCache;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;

/**
 *
 */
public class SimpleIdReaderCache implements IdReaderCache {

    private final Object readerCacheKey;

    private final ImmutableMap<String, SimpleIdReaderTypeCache> types;

    public SimpleIdReaderCache(Object readerCacheKey, ImmutableMap<String, SimpleIdReaderTypeCache> types) {
        this.readerCacheKey = readerCacheKey;
        this.types = types;
    }

    @Override
    public Object readerCacheKey() {
        return this.readerCacheKey;
    }

    @Override
    public IdReaderTypeCache type(String type) {
        return types.get(type);
    }

    @Override
    public HashedBytesArray parentIdByDoc(String type, int docId) {
        SimpleIdReaderTypeCache typeCache = types.get(type);
        if (typeCache != null) {
            return typeCache.parentIdByDoc(docId);
        }
        return null;
    }

    @Override
    public int docById(String type, HashedBytesArray id) {
        SimpleIdReaderTypeCache typeCache = types.get(type);
        if (typeCache != null) {
            return typeCache.docById(id);
        }
        return -1;
    }

    /**
     * Returns an already stored instance if exists, if not, returns null;
     */
    public HashedBytesArray canReuse(HashedBytesArray id) {
        for (SimpleIdReaderTypeCache typeCache : types.values()) {
            HashedBytesArray wrap = typeCache.canReuse(id);
            if (wrap != null) {
                return wrap;
            }
        }
        return null;
    }
}
