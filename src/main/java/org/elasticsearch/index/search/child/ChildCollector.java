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

package org.elasticsearch.index.search.child;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ChildCollector extends Collector {

    private final String parentType;

    private final SearchContext context;

    private final Tuple<IndexReader, IdReaderTypeCache>[] readers;

    private final Map<Object, FixedBitSet> parentDocs;

    private IdReaderTypeCache typeCache;

    public ChildCollector(String parentType, SearchContext context) {
        this.parentType = parentType;
        this.context = context;
        this.parentDocs = new HashMap<Object, FixedBitSet>();

        // create a specific type map lookup for faster lookup operations per doc
        this.readers = new Tuple[context.searcher().subReaders().length];
        for (int i = 0; i < readers.length; i++) {
            IndexReader reader = context.searcher().subReaders()[i];
            readers[i] = new Tuple<IndexReader, IdReaderTypeCache>(reader, context.idCache().reader(reader).type(parentType));
        }
    }

    public Map<Object, FixedBitSet> parentDocs() {
        return this.parentDocs;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {

    }

    @Override
    public void collect(int doc) throws IOException {
        HashedBytesArray parentId = typeCache.parentIdByDoc(doc);
        if (parentId == null) {
            return;
        }
        for (Tuple<IndexReader, IdReaderTypeCache> tuple : readers) {
            IndexReader indexReader = tuple.v1();
            IdReaderTypeCache idReaderTypeCache = tuple.v2();
            if (idReaderTypeCache == null) { // might be if we don't have that doc with that type in this reader
                continue;
            }
            int parentDocId = idReaderTypeCache.docById(parentId);
            if (parentDocId != -1 && !indexReader.isDeleted(parentDocId)) {
                FixedBitSet docIdSet = parentDocs().get(indexReader.getCoreCacheKey());
                if (docIdSet == null) {
                    docIdSet = new FixedBitSet(indexReader.maxDoc());
                    parentDocs.put(indexReader.getCoreCacheKey(), docIdSet);
                }
                docIdSet.set(parentDocId);
                return;
            }
        }
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
        typeCache = context.idCache().reader(reader).type(parentType);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
