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

package org.elasticsearch.search.facet.range;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.script.search.SearchScript;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.support.AbstractFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptRangeFacetCollector extends AbstractFacetCollector {

    private final SearchScript keyScript;

    private final SearchScript valueScript;

    private final RangeFacet.Entry[] entries;

    public ScriptRangeFacetCollector(String facetName, String scriptLang, String keyScript, String valueScript, Map<String, Object> params, RangeFacet.Entry[] entries, SearchContext context) {
        super(facetName);
        this.keyScript = new SearchScript(context.lookup(), scriptLang, keyScript, params, context.scriptService());
        this.valueScript = new SearchScript(context.lookup(), scriptLang, valueScript, params, context.scriptService());
        this.entries = entries;
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        keyScript.setNextReader(reader);
        valueScript.setNextReader(reader);
    }

    @Override protected void doCollect(int doc) throws IOException {
        double key = ((Number) keyScript.execute(doc)).doubleValue();
        double value = ((Number) valueScript.execute(doc)).doubleValue();

        for (RangeFacet.Entry entry : entries) {
            if (key >= entry.getFrom() && key < entry.getTo()) {
                entry.count++;
                entry.total += value;
            }
        }
    }

    @Override public Facet facet() {
        return new InternalRangeFacet(facetName, "_na", "_na", entries);
    }
}
