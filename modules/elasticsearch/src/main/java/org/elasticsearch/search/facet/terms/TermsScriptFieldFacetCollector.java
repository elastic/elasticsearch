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

package org.elasticsearch.search.facet.terms;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.trove.TObjectIntHashMap;
import org.elasticsearch.common.trove.TObjectIntIterator;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.script.search.SearchScript;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.support.AbstractFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author kimchy (shay.banon)
 */
public class TermsScriptFieldFacetCollector extends AbstractFacetCollector {

    private final InternalTermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final String sScript;

    private final SearchScript script;

    private final Matcher matcher;

    private final ImmutableSet<String> excluded;

    private final TObjectIntHashMap<String> facets;

    public TermsScriptFieldFacetCollector(String facetName, int size, InternalTermsFacet.ComparatorType comparatorType, SearchContext context,
                                          ImmutableSet<String> excluded, Pattern pattern, String scriptLang, String script, Map<String, Object> params) {
        super(facetName);
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.sScript = script;
        this.script = new SearchScript(context.lookup(), scriptLang, script, params, context.scriptService());

        this.excluded = excluded;
        this.matcher = pattern != null ? pattern.matcher("") : null;

        this.facets = TermsFacetCollector.popFacets();
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        script.setNextReader(reader);
    }

    @Override protected void doCollect(int doc) throws IOException {
        Object o = script.execute(doc);
        if (o == null) {
            return;
        }
        if (o instanceof Iterable) {
            for (Object o1 : ((Iterable) o)) {
                String value = o1.toString();
                if (match(value)) {
                    facets.adjustOrPutValue(value, 1, 1);
                }
            }
        } else if (o instanceof Object[]) {
            for (Object o1 : ((Object[]) o)) {
                String value = o1.toString();
                if (match(value)) {
                    facets.adjustOrPutValue(value, 1, 1);
                }
            }
        } else {
            String value = o.toString();
            if (match(value)) {
                facets.adjustOrPutValue(value, 1, 1);
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
            TermsFacetCollector.pushFacets(facets);
            return new InternalTermsFacet(facetName, sScript, comparatorType, size, ImmutableList.<InternalTermsFacet.Entry>of());
        } else {
            // we need to fetch facets of "size * numberOfShards" because of problems in how they are distributed across shards
            BoundedTreeSet<InternalTermsFacet.Entry> ordered = new BoundedTreeSet<InternalTermsFacet.Entry>(InternalTermsFacet.ComparatorType.COUNT.comparator(), size * numberOfShards);
            for (TObjectIntIterator<String> it = facets.iterator(); it.hasNext();) {
                it.advance();
                ordered.add(new InternalTermsFacet.Entry(it.key(), it.value()));
            }
            TermsFacetCollector.pushFacets(facets);
            return new InternalTermsFacet(facetName, sScript, comparatorType, size, ordered);
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final ImmutableSet<String> excluded;

        private final Matcher matcher;

        private final SearchScript script;

        private final Map<String, Object> scriptParams;

        public AggregatorValueProc(TObjectIntHashMap<String> facets, ImmutableSet<String> excluded, Pattern pattern, SearchScript script) {
            super(facets);
            this.excluded = excluded;
            this.matcher = pattern != null ? pattern.matcher("") : null;
            this.script = script;
            if (script != null) {
                scriptParams = Maps.newHashMapWithExpectedSize(4);
            } else {
                scriptParams = null;
            }
        }

        @Override public void onValue(int docId, String value) {
            if (excluded != null && excluded.contains(value)) {
                return;
            }
            if (matcher != null && !matcher.reset(value).matches()) {
                return;
            }
            if (script != null) {
                scriptParams.put("term", value);
                Object scriptValue = script.execute(docId, scriptParams);
                if (scriptValue == null) {
                    return;
                }
                if (scriptValue instanceof Boolean) {
                    if (!((Boolean) scriptValue)) {
                        return;
                    }
                } else {
                    value = scriptValue.toString();
                }
            }
            super.onValue(docId, value);
        }
    }

    public static class StaticAggregatorValueProc implements FieldData.StringValueInDocProc {

        private final TObjectIntHashMap<String> facets;

        public StaticAggregatorValueProc(TObjectIntHashMap<String> facets) {
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
