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

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Rescore phase of a search request, used to run potentially expensive scoring models against the top matching documents.
 */
public class RescorePhase extends AbstractComponent implements SearchPhase {

    public RescorePhase(Settings settings) {
        super(settings);
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) {
        try {
            TopDocs topDocs = context.queryResult().topDocs();
            for (RescoreContext ctx : context.rescore()) {
                topDocs = ctx.rescorer().rescore(topDocs, context.searcher(), ctx);
                // It is the responsibility of the rescorer to sort the resulted top docs,
                // here we only assert that this condition is met.
                assert context.sort() == null && topDocsSortedByScore(topDocs): "topdocs should be sorted after rescore";
            }
            context.queryResult().topDocs(topDocs, context.queryResult().sortValueFormats());
        } catch (IOException e) {
            throw new ElasticsearchException("Rescore Phase Failed", e);
        }
    }

    /**
     * Returns true if the provided docs are sorted by score.
     */
    private boolean topDocsSortedByScore(TopDocs topDocs) {
        if (topDocs == null || topDocs.scoreDocs == null || topDocs.scoreDocs.length < 2) {
            return true;
        }
        float lastScore = topDocs.scoreDocs[0].score;
        for (int i = 1; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc doc = topDocs.scoreDocs[i];
            if (Float.compare(doc.score, lastScore) > 0) {
                return false;
            }
            lastScore = doc.score;
        }
        return true;
    }
}
