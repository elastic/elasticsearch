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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query rescorer interface used to re-rank the Top-K results of a previously
 * executed search.
 */
public interface Rescorer {

    /**
     * Returns the name of this rescorer
     */
    public String name();

    /**
     * Modifies the result of the previously executed search ({@link TopDocs})
     * in place based on the given {@link RescoreSearchContext}.
     *
     * @param topDocs        the result of the previously executed search
     * @param context        the current {@link SearchContext}. This will never be <code>null</code>.
     * @param rescoreContext the {@link RescoreSearchContext}. This will never be <code>null</code>
     * @throws IOException if an {@link IOException} occurs during rescoring
     */
    public TopDocs rescore(TopDocs topDocs, SearchContext context, RescoreSearchContext rescoreContext) throws IOException;

    /**
     * Executes an {@link Explanation} phase on the rescorer.
     *
     * @param topLevelDocId the global / top-level document ID to explain
     * @param context the explanation for the results being fed to this rescorer
     * @param rescoreContext context for this rescorer
     * @param sourceExplanation explanation of the source of the documents being fed into this rescore
     * @return the explain for the given top level document ID.
     * @throws IOException if an {@link IOException} occurs
     */
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
            Explanation sourceExplanation) throws IOException;

    /**
     * Extracts all terms needed to execute this {@link Rescorer}. This method
     * is executed in a distributed frequency collection roundtrip for
     * {@link SearchType#DFS_QUERY_AND_FETCH} and
     * {@link SearchType#DFS_QUERY_THEN_FETCH}
     */
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet);

    /*
     * TODO: At this point we only have one implementation which modifies the
     * TopDocs given. Future implementations might return actual results that
     * contain information about the rescore context. For example a pair wise
     * reranker might return the feature vector for the top N window in order to
     * merge results on the callers side. For now we don't have a return type at
     * all since something like this requires a more general refactoring how
     * documents are merged since in such a case we don't really have a score
     * per document rather a "X is more relevant than Y" relation
     */

}
