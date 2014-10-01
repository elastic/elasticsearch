/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.search.facet.range;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ScriptRangeFacetExecutor extends FacetExecutor {

    final SearchScript keyScript;
    final SearchScript valueScript;

    private final RangeFacet.Entry[] entries;

    public ScriptRangeFacetExecutor(String scriptLang, String keyScript, ScriptService.ScriptType keyScriptType, String valueScript, ScriptService.ScriptType valueScriptType, Map<String, Object> params, RangeFacet.Entry[] entries, SearchContext context) {
        this.keyScript = context.scriptService().search(context.lookup(), scriptLang, keyScript, keyScriptType, params);
        this.valueScript = context.scriptService().search(context.lookup(), scriptLang, valueScript, valueScriptType, params);
        this.entries = entries;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalRangeFacet(facetName, entries);
    }

    class Collector extends FacetExecutor.Collector {

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            keyScript.setScorer(scorer);
            valueScript.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyScript.setNextReader(context);
            valueScript.setNextReader(context);
        }

        @Override
        public void collect(int doc) throws IOException {
            keyScript.setNextDocId(doc);
            valueScript.setNextDocId(doc);
            double key = keyScript.runAsDouble();
            double value = valueScript.runAsDouble();

            for (RangeFacet.Entry entry : entries) {
                if (key >= entry.getFrom() && key < entry.getTo()) {
                    entry.count++;
                    entry.totalCount++;
                    entry.total += value;
                    if (value < entry.min) {
                        entry.min = value;
                    }
                    if (value > entry.max) {
                        entry.max = value;
                    }
                }
            }
        }

        @Override
        public void postCollection() {
        }
    }
}
