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
package org.apache.lucene5_shaded.search;

import java.io.IOException;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.Bits;

/** 
 * Constrains search results to only match those which also match a provided
 * query.  
 *
 * <p> This could be used, for example, with a {@link NumericRangeQuery} on a suitably
 * formatted date field to implement date filtering.  One could re-use a single
 * CachingWrapperFilter(QueryWrapperFilter) that matches, e.g., only documents modified 
 * within the last week.  This would only need to be reconstructed once per day.
 * @deprecated You can use {@link Query} objects directly for filtering by using
 * {@link BooleanClause.Occur#FILTER} clauses in a {@link BooleanQuery}.
 */
@Deprecated
public class QueryWrapperFilter extends Filter {
  private final Query query;

  /** Constructs a filter which only matches documents matching
   * <code>query</code>.
   */
  public QueryWrapperFilter(Query query) {
    if (query == null)
      throw new NullPointerException("Query may not be null");
    this.query = query;
  }
  
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return new BoostQuery(new ConstantScoreQuery(query), 0f);
  }
  
  /** returns the inner Query */
  public final Query getQuery() {
    return query;
  }

  @Override
  public DocIdSet getDocIdSet(final LeafReaderContext context, final Bits acceptDocs) throws IOException {
    // get a private context that is used to rewrite, createWeight and score eventually
    final LeafReaderContext privateContext = context.reader().getContext();
    final Weight weight = new IndexSearcher(privateContext).createNormalizedWeight(query, false);
    
    DocIdSet set = new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        Scorer s = weight.scorer(privateContext);
        return s == null ? null : s.iterator();
      }

      @Override
      public long ramBytesUsed() {
        return 0L;
      }
    };
    return BitsFilteredDocIdSet.wrap(set, acceptDocs);
  }

  @Override
  public String toString(String field) {
    return "QueryWrapperFilter(" + query.toString(field) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (super.equals(o) == false) {
      return false;
    }
    return this.query.equals(((QueryWrapperFilter)o).query);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + query.hashCode();
  }
}
