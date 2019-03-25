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

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;

/**
 * A query rescorer interface used to re-rank the Top-K results of a previously
 * executed search.
 *
 * Subclasses should borrow heavily from {@link QueryRescorer} because it is
 * fairly well behaved and documents that tradeoffs that it is making. There
 * is also an {@code ExampleRescorer} that is worth looking at. 
 */
public interface Rescorer {
    /**
     * Modifies the result of the previously executed search ({@link TopDocs})
     * in place based on the given {@link RescoreContext}.
     *
     * @param topDocs        the result of the previously executed search
     * @param searcher       the searcher used for this search. This will never be <code>null</code>.
     * @param rescoreContext the {@link RescoreContext}. This will never be <code>null</code>
     * @throws IOException if an {@link IOException} occurs during rescoring
     */
    TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException;

    /**
     * Executes an {@link Explanation} phase on the rescorer.
     *
     * @param topLevelDocId the global / top-level document ID to explain
     * @param searcher the searcher used for this search. This will never be <code>null</code>.
     * @param rescoreContext context for this rescorer
     * @param sourceExplanation explanation of the source of the documents being fed into this rescore
     * @return the explain for the given top level document ID.
     * @throws IOException if an {@link IOException} occurs
     */
    Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext,
                        Explanation sourceExplanation) throws IOException;

}
