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
 *  Convenient base class for building queries that only perform matching, but
 *  no scoring. The scorer produced by such queries always returns 0 as score.
 *
 *  @deprecated Use {@link Query} objects instead: when queries are wrapped in a
 *  {@link ConstantScoreQuery} or in a {@link BooleanClause.Occur#FILTER} clause,
 *  they automatically disable the score computation so the {@link Filter} class
 *  does not provide benefits compared to queries anymore.
 */
@Deprecated
public abstract class Filter extends Query {

  /**
   * Creates a {@link DocIdSet} enumerating the documents that should be
   * permitted in search results. <b>NOTE:</b> null can be
   * returned if no documents are accepted by this Filter.
   * <p>
   * Note: This method will be called once per segment in
   * the index during searching.  The returned {@link DocIdSet}
   * must refer to document IDs for that segment, not for
   * the top-level reader.
   *
   * @param context a {@link LeafReaderContext} instance opened on the index currently
   *         searched on. Note, it is likely that the provided reader info does not
   *         represent the whole underlying index i.e. if the index has more than
   *         one segment the given reader only represents a single segment.
   *         The provided context is always an atomic context, so you can call
   *         {@link org.apache.lucene5_shaded.index.LeafReader#fields()}
   *         on the context's reader, for example.
   *
   * @param acceptDocs
   *          Bits that represent the allowable docs to match (typically deleted docs
   *          but possibly filtering other documents)
   *
   * @return a DocIdSet that provides the documents which should be permitted or
   *         prohibited in search results. <b>NOTE:</b> <code>null</code> should be returned if
   *         the filter doesn't accept any documents otherwise internal optimization might not apply
   *         in the case an <i>empty</i> {@link DocIdSet} is returned.
   */
  public abstract DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException;

  //
  // Query compatibility
  //

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    return FilteredQuery.RANDOM_ACCESS_FILTER_STRATEGY.rewrite(this);
  }
}
