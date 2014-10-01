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

package org.elasticsearch.search.facet.statistical;

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
public class ScriptStatisticalFacetExecutor extends FacetExecutor {

    private final SearchScript script;

    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double total = 0;
    private double sumOfSquares = 0.0;
    private long count;

    public ScriptStatisticalFacetExecutor(String scriptLang, String script, ScriptService.ScriptType scriptType, Map<String, Object> params, SearchContext context) {
        this.script = context.scriptService().search(context.lookup(), scriptLang, script, scriptType, params);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalStatisticalFacet(facetName, min, max, total, sumOfSquares, count);
    }

    class Collector extends FacetExecutor.Collector {

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double total = 0;
        private double sumOfSquares = 0.0;
        private long count;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            script.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            script.setNextReader(context);
        }

        @Override
        public void collect(int doc) throws IOException {
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

        @Override
        public void postCollection() {
            ScriptStatisticalFacetExecutor.this.min = min;
            ScriptStatisticalFacetExecutor.this.max = max;
            ScriptStatisticalFacetExecutor.this.total = total;
            ScriptStatisticalFacetExecutor.this.sumOfSquares = sumOfSquares;
            ScriptStatisticalFacetExecutor.this.count = count;
        }
    }
}