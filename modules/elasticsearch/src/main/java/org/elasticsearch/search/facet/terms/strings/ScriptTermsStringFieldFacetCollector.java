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

package org.elasticsearch.search.facet.terms.strings;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.trove.iterator.TObjectIntIterator;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptTermsStringFieldFacetCollector extends AbstractFacetCollector {

    private final InternalStringTermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final SearchScript script;

    private final Matcher matcher;

    private final ImmutableSet<String> excluded;

    private final TObjectIntHashMap<String> facets;

    private int missing;

    public ScriptTermsStringFieldFacetCollector(String facetName, int size, InternalStringTermsFacet.ComparatorType comparatorType, SearchContext context,
                                                ImmutableSet<String> excluded, Pattern pattern, String scriptLang, String script, Map<String, Object> params) {
        super(facetName);
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);

        this.excluded = excluded;
        this.matcher = pattern != null ? pattern.matcher("") : null;

        this.facets = CacheRecycler.popObjectIntMap();
    }

    @Override public void setScorer(Scorer scorer) throws IOException {
        script.setScorer(scorer);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        script.setNextReader(reader);
    }

    @Override protected void doCollect(int doc) throws IOException {
        script.setNextDocId(doc);
        Object o = script.run();
        if (o == null) {
            missing++;
            return;
        }
        if (o instanceof Iterable) {
            boolean found = false;
            for (Object o1 : ((Iterable) o)) {
                String value = o1.toString();
                if (match(value)) {
                    found = true;
                    facets.adjustOrPutValue(value, 1, 1);
                }
            }
            if (!found) {
                missing++;
            }
        } else if (o instanceof Object[]) {
            boolean found = false;
            for (Object o1 : ((Object[]) o)) {
                String value = o1.toString();
                if (match(value)) {
                    found = true;
                    facets.adjustOrPutValue(value, 1, 1);
                }
            }
            if (!found) {
                missing++;
            }
        } else {
            String value = o.toString();
            if (match(value)) {
                facets.adjustOrPutValue(value, 1, 1);
            } else {
                missing++;
            }
        }
    }

    private boolean match(String value) {
        if (excluded != null && excluded.contains(value)) {
            return false;
        }
        if (matcher != null && !matcher.reset(value).matches()) {
            return false;
        }
        return true;
    }

    @Override public Facet facet() {
        if (facets.isEmpty()) {
            CacheRecycler.pushObjectIntMap(facets);
            return new InternalStringTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalStringTermsFacet.StringEntry>of(), missing);
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                for (TObjectIntIterator<String> it = facets.iterator(); it.hasNext();) {
                    it.advance();
                    ordered.insertWithOverflow(new InternalStringTermsFacet.StringEntry(it.key(), it.value()));
                }
                InternalStringTermsFacet.StringEntry[] list = new InternalStringTermsFacet.StringEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = ((InternalStringTermsFacet.StringEntry) ordered.pop());
                }
                CacheRecycler.pushObjectIntMap(facets);
                return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing);
            } else {
                BoundedTreeSet<InternalStringTermsFacet.StringEntry> ordered = new BoundedTreeSet<InternalStringTermsFacet.StringEntry>(comparatorType.comparator(), size);
                for (TObjectIntIterator<String> it = facets.iterator(); it.hasNext();) {
                    it.advance();
                    ordered.add(new InternalStringTermsFacet.StringEntry(it.key(), it.value()));
                }
                CacheRecycler.pushObjectIntMap(facets);
                return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, missing);
            }
        }
    }
}
