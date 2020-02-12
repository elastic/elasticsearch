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
package org.elasticsearch.search.fetch.subphase.matches;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

public final class MatchesFetchSubPhase implements FetchSubPhase {

    private final Map<String, MatchesProcessor> processors;

    public MatchesFetchSubPhase(Map<String, MatchesProcessor> processors) {
        this.processors = processors;
    }

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {

        MatchesContext mc = context.matches();
        if (mc == null) {
            return;
        }

        Query q = getQuery(context);
        Weight w = context.searcher().createWeight(context.searcher().rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1);
        SearchHit[] sortedHits = hits.clone();
        Arrays.sort(sortedHits, Comparator.comparingInt(SearchHit::docId));
        for (SearchHit hit : sortedHits) {
            int ord = ReaderUtil.subIndex(hit.docId(), context.searcher().getTopReaderContext().leaves());
            LeafReaderContext ctx = context.searcher().getTopReaderContext().leaves().get(ord);
            int doc = hit.docId() - ctx.docBase;
            Matches m = w.matches(ctx, doc);
            if (m == null) {
                continue;
            }
            for (String process : mc.getMatchProcesses()) {
                MatchesProcessor processor = processors.get(process);
                if (processor == null) {
                    throw new IllegalArgumentException("Unknown match processor [" + process + "]");
                }
                hit.addMatchResult(processor.process(m, mc.getProcessSettings(process)));
            }
        }
    }

    private Query getQuery(SearchContext context) {
        if (context instanceof SubSearchContext) {
            return context.query();
        }
        Query q = context.originalQuery();
        // If the query has a named post filter then we need to include that as well
        if (context.postFilter() != null) {
            q = new BooleanQuery.Builder()
                .add(q, BooleanClause.Occur.MUST)
                .add(context.postFilter(), BooleanClause.Occur.FILTER)
                .build();
        }
        return q;
    }

}
