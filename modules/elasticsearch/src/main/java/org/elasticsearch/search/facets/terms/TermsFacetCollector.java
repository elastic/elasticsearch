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

package org.elasticsearch.search.facets.terms;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.trove.TObjectIntHashMap;
import org.elasticsearch.common.trove.TObjectIntIterator;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.support.AbstractFacetCollector;
import org.elasticsearch.util.ThreadLocals;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.elasticsearch.index.field.data.FieldDataOptions.*;

/**
 * @author kimchy (shay.banon)
 */
public class TermsFacetCollector extends AbstractFacetCollector {

    private static ThreadLocal<ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>> cache = new ThreadLocal<ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>>() {
        @Override protected ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>> initialValue() {
            return new ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<java.lang.String>>>(new ArrayDeque<TObjectIntHashMap<String>>());
        }
    };


    private final FieldDataCache fieldDataCache;

    private final String fieldName;

    private final String indexFieldName;

    private final int size;

    private final FieldData.Type fieldDataType;

    private FieldData fieldData;

    private final AggregatorValueProc aggregator;

    public TermsFacetCollector(String facetName, String fieldName, int size, FieldDataCache fieldDataCache, MapperService mapperService) {
        super(facetName);
        this.fieldDataCache = fieldDataCache;
        this.size = size;

        FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName);
        this.fieldName = fieldName;
        if (mapper != null) {
            this.indexFieldName = mapper.names().indexName();
            this.fieldDataType = mapper.fieldDataType();
        } else {
            this.indexFieldName = fieldName;
            this.fieldDataType = FieldData.Type.STRING;
        }

        aggregator = new AggregatorValueProc(popFacets());
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = fieldDataCache.cache(fieldDataType, reader, indexFieldName, fieldDataOptions().withFreqs(false));
    }

    @Override protected void doCollect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, aggregator);
    }

    @Override public Facet facet() {
        TObjectIntHashMap<String> facets = aggregator.facets();
        if (facets.isEmpty()) {
            pushFacets(facets);
            return new InternalTermsFacet(facetName, fieldName, InternalTermsFacet.ComparatorType.COUNT, size, ImmutableList.<InternalTermsFacet.Entry>of());
        } else {
            BoundedTreeSet<InternalTermsFacet.Entry> ordered = new BoundedTreeSet<InternalTermsFacet.Entry>(InternalTermsFacet.ComparatorType.COUNT.comparator(), size);
            for (TObjectIntIterator<String> it = facets.iterator(); it.hasNext();) {
                it.advance();
                ordered.add(new InternalTermsFacet.Entry(it.key(), it.value()));
            }
            pushFacets(facets);
            return new InternalTermsFacet(facetName, fieldName, InternalTermsFacet.ComparatorType.COUNT, size, ordered);
        }
    }

    private TObjectIntHashMap<String> popFacets() {
        Deque<TObjectIntHashMap<String>> deque = cache.get().get();
        if (deque.isEmpty()) {
            deque.add(new TObjectIntHashMap<String>());
        }
        TObjectIntHashMap<String> facets = deque.pollFirst();
        facets.clear();
        return facets;
    }

    private void pushFacets(TObjectIntHashMap<String> facets) {
        facets.clear();
        Deque<TObjectIntHashMap<String>> deque = cache.get().get();
        if (deque != null) {
            deque.add(facets);
        }
    }

    public static class AggregatorValueProc implements FieldData.StringValueInDocProc {

        private final TObjectIntHashMap<String> facets;

        public AggregatorValueProc(TObjectIntHashMap<String> facets) {
            this.facets = facets;
        }

        @Override public void onValue(int docId, String value) {
            facets.adjustOrPutValue(value, 1, 1);
        }

        public final TObjectIntHashMap<String> facets() {
            return facets;
        }
    }
}
