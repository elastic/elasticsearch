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
package org.elasticsearch.index.search.child;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.SearchContext;

/**
 * A simple collector that only collects if the docs parent ID is not
 * <code>null</code>
 */
abstract class ParentIdCollector extends NoopCollector {
    protected final String type;
    protected final SearchContext context;
    private IdReaderTypeCache typeCache;

    protected ParentIdCollector(String parentType, SearchContext context) {
        this.type = parentType;
        this.context = context;
    }

    @Override
    public final void collect(int doc) throws IOException {
        if (typeCache != null) {
            HashedBytesArray parentIdByDoc = typeCache.parentIdByDoc(doc);
            if (parentIdByDoc != null) {
               collect(doc, parentIdByDoc);
            }
        }
    }
    
    protected abstract void collect(int doc, HashedBytesArray parentId) throws IOException;

    @Override
    public void setNextReader(AtomicReaderContext readerContext) throws IOException {
        typeCache = context.idCache().reader(readerContext.reader()).type(type);
    }
}
