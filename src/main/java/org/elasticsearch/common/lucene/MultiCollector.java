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

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.elasticsearch.common.lucene.search.XCollector;

import java.io.IOException;

/**
 *
 */
public class MultiCollector extends SimpleCollector implements XCollector {

    private final Collector collector;
    private final Collector[] collectors;

    private LeafCollector leafCollector;
    private final LeafCollector[] leafCollectors;


    public MultiCollector(Collector collector, Collector[] collectors) {
        this.collector = collector;
        this.collectors = collectors;
        this.leafCollectors = new LeafCollector[collectors.length];
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        // always wrap it in a scorer wrapper
        if (!(scorer instanceof ScoreCachingWrappingScorer)) {
            scorer = new ScoreCachingWrappingScorer(scorer);
        }
        leafCollector.setScorer(scorer);
        for (LeafCollector leafCollector : leafCollectors) {
            leafCollector.setScorer(scorer);
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        leafCollector.collect(doc);
        for (LeafCollector leafCollector : leafCollectors) {
            leafCollector.collect(doc);
        }
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        leafCollector = collector.getLeafCollector(context);
        for (int i = 0; i < collectors.length; i++) {
            leafCollectors[i] = collectors[i].getLeafCollector(context);
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        if (!leafCollector.acceptsDocsOutOfOrder()) {
            return false;
        }
        for (LeafCollector leafCollector : leafCollectors) {
            if (!leafCollector.acceptsDocsOutOfOrder()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void postCollection() throws IOException {
        if (collector instanceof XCollector) {
            ((XCollector) collector).postCollection();
        }
        for (Collector collector : collectors) {
            if (collector instanceof XCollector) {
                ((XCollector) collector).postCollection();
            }
        }
    }
}
