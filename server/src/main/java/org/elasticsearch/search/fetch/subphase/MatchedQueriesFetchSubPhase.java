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
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.query.NamedQuery;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public final class MatchedQueriesFetchSubPhase implements FetchSubPhase {

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws IOException {

        if ((context.parsedQuery() == null || context.parsedQuery().matchNamedQueries() == false) && (
            context.parsedPostFilter() == null || context.parsedPostFilter().matchNamedQueries() == false)) {
            return;
        }

        Weight w = getWeight(context, hitContext);
        Matches m = w.matches(hitContext.readerContext(), hitContext.docId());

        if (m == null) {
            return;
        }

        for (NamedQuery.NamedMatches nm : NamedQuery.findNamedMatches(m)) {
            hitContext.hit().addMatchedQuery(nm.getName());
        }

    }

    private Weight getWeight(SearchContext context, HitContext hitContext) throws IOException {
        if (hitContext.cache().containsKey("weight")) {
            return (Weight) hitContext.cache().get("weight");
        }
        Query q = context.parsedQuery().query();
        // If the query has a named post filter then we need to include that as well
        if (context.parsedPostFilter() != null && context.parsedPostFilter().matchNamedQueries()) {
            q = new BooleanQuery.Builder()
                .add(q, BooleanClause.Occur.MUST)
                .add(context.parsedPostFilter().query(), BooleanClause.Occur.FILTER)
                .build();
        }
        Weight w = context.searcher().createWeight(context.searcher().rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1);
        hitContext.cache().put("weight", w);
        return w;
    }

}
