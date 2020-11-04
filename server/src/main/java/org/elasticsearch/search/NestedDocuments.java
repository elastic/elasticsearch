/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class NestedDocuments {

    private final Map<String, BitSetProducer> parentObjectFilters = new HashMap<>();
    private final BitSetProducer parentDocumentFilter;
    private final MapperService mapperService;

    public NestedDocuments(MapperService mapperService, Function<Query, BitSetProducer> filterCache) {
        this.mapperService = mapperService;
        if (mapperService.hasNested() == false) {
            this.parentDocumentFilter = null;
        } else {
            this.parentDocumentFilter = filterCache.apply(Queries.newNonNestedFilter());
            for (ObjectMapper mapper : mapperService.documentMapper().mappers().objectMappers().values()) {
                if (mapper.nested().isNested() == false) {
                    continue;
                }
                parentObjectFilters.put(mapper.name(),
                    filterCache.apply(mapper.nestedTypeFilter()));
            }
        }
    }

    public LeafNestedDocuments getLeafNestedDocuments(LeafReaderContext ctx) throws IOException {
        if (parentDocumentFilter == null) {
            return LeafNestedDocuments.NO_NESTED_MAPPERS;
        }
        return new HasNestedDocuments(ctx);
    }

    private class HasNestedDocuments implements LeafNestedDocuments {

        final LeafReaderContext ctx;
        final BitSet parentFilter;
        final Map<String, BitSet> objectFilters = new HashMap<>();

        int doc = -1;
        int rootDoc = -1;
        SearchHit.NestedIdentity nestedIdentity = null;

        private HasNestedDocuments(LeafReaderContext ctx) throws IOException {
            this.ctx = ctx;
            this.parentFilter = parentDocumentFilter.getBitSet(ctx);
            for (Map.Entry<String, BitSetProducer> filter : parentObjectFilters.entrySet()) {
                objectFilters.put(filter.getKey(), filter.getValue().getBitSet(ctx));
            }
        }

        @Override
        public boolean advance(int doc) throws IOException {
            if (parentFilter.get(doc)) {
                // parent doc, no nested identity
                this.nestedIdentity = null;
                this.doc = doc;
                this.rootDoc = doc;
                return false;
            } else {
                this.doc = doc;
                this.rootDoc = parentFilter.nextSetBit(doc);
                this.nestedIdentity = loadNestedIdentity();
                return true;
            }
        }

        @Override
        public int doc() {
            assert doc != -1 : "Called doc() when unpositioned";
            return doc;
        }

        @Override
        public int rootDoc() {
            assert doc != -1 : "Called rootDoc() when unpositioned";
            return rootDoc;
        }

        @Override
        public SearchHit.NestedIdentity nestedIdentity() {
            assert doc != -1 : "Called nestedIdentity() when unpositioned";
            return nestedIdentity;
        }

        @Override
        public boolean hasNonNestedParent(String path) {
            return mapperService.documentMapper().hasNonNestedParent(path);
        }

        private SearchHit.NestedIdentity loadNestedIdentity() {
            String path = null;
            for (Map.Entry<String, BitSet> objectFilter : objectFilters.entrySet()) {
                if (objectFilter.getValue().get(doc)) {
                    if (path == null || path.length() > objectFilter.getKey().length()) {
                        path = objectFilter.getKey();
                    }
                }
            }
            if (path == null) {
                throw new IllegalStateException("Cannot find object path for document " + doc);
            }
            SearchHit.NestedIdentity ni = null;
            int currentLevelDoc = doc;
            while (path != null) {
                String parent = mapperService.documentMapper().getNestedParent(path);
                BitSet childBitSet = objectFilters.get(path);
                if (childBitSet == null) {
                    throw new IllegalStateException("Cannot find object mapper for path " + path + " in doc " + doc);
                }
                BitSet parentBitSet;
                if (parent == null) {
                    parentBitSet = parentFilter;
                } else {
                    if (objectFilters.containsKey(parent) == false) {
                        throw new IllegalStateException("Cannot find parent mapper for path " + path + " in doc " + doc);
                    }
                    parentBitSet = objectFilters.get(parent);
                }
                int lastParent = parentBitSet.prevSetBit(currentLevelDoc);
                int offset = 0;
                for (int i = lastParent + 1; i < currentLevelDoc; i = childBitSet.nextSetBit(i + 1)) {
                    offset++;
                }
                ni = new SearchHit.NestedIdentity(path.substring(path.indexOf(".") + 1), offset, ni);
                path = parent;
                currentLevelDoc = parentBitSet.nextSetBit(currentLevelDoc);
            }
            return ni;
        }
    }

}
