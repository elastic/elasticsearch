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
package org.elasticsearch.search.fetch.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreSearchContext;

import java.io.IOException;

/**
 *
 */
public final class ExplainFetchSubPhase implements FetchSubPhase {

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.explain() == false) {
            return;
        }
        try {
            final int topLevelDocId = hitContext.hit().docId();
            Explanation explanation = context.searcher().explain(context.query(), topLevelDocId);

            for (RescoreSearchContext rescore : context.rescore()) {
                explanation = rescore.rescorer().explain(topLevelDocId, context, rescore, explanation);
            }
            // we use the top level doc id, since we work with the top level searcher
            hitContext.hit().explanation(explanation);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to explain doc [" + hitContext.hit().type() + "#" + hitContext.hit().id() + "]", e);
        } finally {
            context.clearReleasables(SearchContext.Lifetime.COLLECTION);
        }
    }
}
