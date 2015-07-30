package org.apache.lucene.search.suggest.xdocument;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * Adds document suggest capabilities to IndexSearcher.
 * Any {@link CompletionQuery} can be used to suggest documents.
 *
 * Use {@link PrefixCompletionQuery} for analyzed prefix queries,
 * {@link RegexCompletionQuery} for regular expression prefix queries,
 * {@link FuzzyCompletionQuery} for analyzed prefix with typo tolerance
 * and {@link ContextQuery} to boost and/or filter suggestions by contexts
 *
 * @lucene.experimental
 */
public class SuggestIndexSearcher extends IndexSearcher {

  /**
   * Creates a searcher with document suggest capabilities
   * for <code>reader</code>.
   */
  public SuggestIndexSearcher(IndexReader reader) {
    super(reader);
  }

  /**
   * Returns top <code>n</code> completion hits for
   * <code>query</code>
   */
  public TopSuggestDocs suggest(CompletionQuery query, int n) throws IOException {
    org.apache.lucene.search.suggest.xdocument.TopSuggestDocsCollector collector = new org.apache.lucene.search.suggest.xdocument.TopSuggestDocsCollector(n);
    suggest(query, collector);
    return collector.get();
  }

  /**
   * Lower-level suggest API.
   * Collects completion hits through <code>collector</code> for <code>query</code>.
   *
   * <p>{@link org.apache.lucene.search.suggest.xdocument.TopSuggestDocsCollector#collect(int, CharSequence, CharSequence, float)}
   * is called for every matching completion hit.
   */
  public void suggest(CompletionQuery query, org.apache.lucene.search.suggest.xdocument.TopSuggestDocsCollector collector) throws IOException {
    // TODO use IndexSearcher.rewrite instead
    // have to implement equals() and hashCode() in CompletionQuerys and co
    query = (CompletionQuery) query.rewrite(getIndexReader());
    Weight weight = query.createWeight(this, collector.needsScores());
    for (LeafReaderContext context : getIndexReader().leaves()) {
      BulkScorer scorer = weight.bulkScorer(context, context.reader().getLiveDocs());
      if (scorer != null) {
        try {
          scorer.score(collector.getLeafCollector(context));
        } catch (CollectionTerminatedException e) {
          // collection was terminated prematurely
          // continue with the following leaf
        }
      }
    }
  }
}
