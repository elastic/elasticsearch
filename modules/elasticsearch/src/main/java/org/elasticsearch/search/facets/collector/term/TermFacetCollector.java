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

package org.elasticsearch.search.facets.collector.term;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.index.cache.field.FieldDataCache;
import org.elasticsearch.index.field.strings.StringFieldData;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.MultiCountFacet;
import org.elasticsearch.search.facets.collector.FacetCollector;
import org.elasticsearch.search.facets.internal.InternalMultiCountFacet;
import org.elasticsearch.util.BoundedTreeSet;
import org.elasticsearch.util.ThreadLocals;
import org.elasticsearch.util.collect.ImmutableList;
import org.elasticsearch.util.gnu.trove.TObjectIntHashMap;
import org.elasticsearch.util.gnu.trove.TObjectIntIterator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.elasticsearch.index.field.FieldDataOptions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TermFacetCollector extends FacetCollector {

    private static ThreadLocal<ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>> cache = new ThreadLocal<ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>>>() {
        @Override protected ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<String>>> initialValue() {
            return new ThreadLocals.CleanableValue<Deque<TObjectIntHashMap<java.lang.String>>>(new ArrayDeque<TObjectIntHashMap<String>>());
        }
    };


    private final FieldDataCache fieldDataCache;

    private final String name;

    private final String fieldName;

    private final int size;

    private StringFieldData fieldData;

    private final TObjectIntHashMap<String> facets;

    public TermFacetCollector(String name, String fieldName, FieldDataCache fieldDataCache, int size) {
        this.name = name;
        this.fieldDataCache = fieldDataCache;
        this.fieldName = fieldName;
        this.size = size;
        facets = popFacets();
    }

    @Override public void setScorer(Scorer scorer) throws IOException {
        // nothing to do here
    }

    @Override public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = fieldDataCache.cache(StringFieldData.class, reader, fieldName, fieldDataOptions().withFreqs(false));
    }

    @Override public void collect(int doc) throws IOException {
        if (fieldData.multiValued()) {
            for (String value : fieldData.values(doc)) {
                facets.adjustOrPutValue(value, 1, 1);
            }
        } else {
            if (fieldData.hasValue(doc)) {
                facets.adjustOrPutValue(fieldData.value(doc), 1, 1);
            }
        }
    }

    @Override public Facet facet() {
        if (facets.isEmpty()) {
            pushFacets(facets);
            return new InternalMultiCountFacet<String>(name, MultiCountFacet.ValueType.STRING, MultiCountFacet.ComparatorType.COUNT, size, ImmutableList.<MultiCountFacet.Entry<String>>of());
        } else {
            BoundedTreeSet<MultiCountFacet.Entry<String>> ordered = new BoundedTreeSet<MultiCountFacet.Entry<String>>(MultiCountFacet.ComparatorType.COUNT.comparator(), size);
            for (TObjectIntIterator<String> it = facets.iterator(); it.hasNext();) {
                it.advance();
                ordered.add(new MultiCountFacet.Entry<String>(it.key(), it.value()));
            }
            pushFacets(facets);
            return new InternalMultiCountFacet<String>(name, MultiCountFacet.ValueType.STRING, MultiCountFacet.ComparatorType.COUNT, size, ordered);
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
}
