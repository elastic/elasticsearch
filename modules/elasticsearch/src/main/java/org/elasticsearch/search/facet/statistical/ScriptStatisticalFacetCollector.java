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

package org.elasticsearch.search.facet.statistical;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptStatisticalFacetCollector extends AbstractFacetCollector {

    private final SearchScript script;

    private double min = Double.POSITIVE_INFINITY;

    private double max = Double.NEGATIVE_INFINITY;

    private double total = 0;

    private double sumOfSquares = 0.0;

    private long count;

    public ScriptStatisticalFacetCollector(String facetName, String scriptLang, String script, Map<String, Object> params, SearchContext context) {
        super(facetName);
        this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
    }

    @Override protected void doCollect(int doc) throws IOException {
        script.setNextDocId(doc);
        double value = script.runAsDouble();
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
        sumOfSquares += value * value;
        total += value;
        count++;
    }

    @Override public void setScorer(Scorer scorer) throws IOException {
        script.setScorer(scorer);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        script.setNextReader(reader);
    }

    @Override public Facet facet() {
        return new InternalStatisticalFacet(facetName, min, max, total, sumOfSquares, count);
    }
}