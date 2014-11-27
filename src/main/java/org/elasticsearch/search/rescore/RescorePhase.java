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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 */
public class RescorePhase extends AbstractComponent implements SearchPhase {
    
    @Inject
    public RescorePhase(Settings settings) {
        super(settings);
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("rescore", new RescoreParseElement());
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) throws ElasticsearchException {
        try {
            TopDocs topDocs = context.queryResult().topDocs();
            for (RescoreSearchContext ctx : context.rescore()) {
                topDocs = ctx.rescorer().rescore(topDocs, context, ctx);
            }
            if (context.size() < topDocs.scoreDocs.length) {
                ScoreDoc[] hits = new ScoreDoc[context.size()];
                System.arraycopy(topDocs.scoreDocs, 0, hits, 0, hits.length);
                topDocs = new TopDocs(topDocs.totalHits, hits, topDocs.getMaxScore());
            }
            context.queryResult().topDocs(topDocs);
        } catch (IOException e) {
            throw new ElasticsearchException("Rescore Phase Failed", e);
        }
    }
}
