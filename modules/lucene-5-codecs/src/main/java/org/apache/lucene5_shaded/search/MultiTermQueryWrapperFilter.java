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

import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.BitDocIdSet;
import org.apache.lucene5_shaded.util.Bits;

/**
 * A wrapper for {@link MultiTermQuery}, that exposes its
 * functionality as a {@link Filter}.
 * <P>
 * <code>MultiTermQueryWrapperFilter</code> is not designed to
 * be used by itself. Normally you subclass it to provide a Filter
 * counterpart for a {@link MultiTermQuery} subclass.
 * <P>
 * For example, {@link TermRangeFilter} and {@link PrefixFilter} extend
 * <code>MultiTermQueryWrapperFilter</code>.
 * @deprecated Use {@link MultiTermQueryConstantScoreWrapper} instead
 */
@Deprecated
public class MultiTermQueryWrapperFilter<Q extends MultiTermQuery> extends Filter {

  protected final Q query;

  /**
   * Wrap a {@link MultiTermQuery} as a Filter.
   */
  protected MultiTermQueryWrapperFilter(Q query) {
      this.query = query;
  }

  @Override
  public String toString(String field) {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString(field);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public final boolean equals(final Object o) {
    if (o==this) return true;
    if (super.equals(o) == false) {
      return false;
    }
    return this.query.equals( ((MultiTermQueryWrapperFilter)o).query );
  }

  @Override
  public final int hashCode() {
    return 31 * super.hashCode() + query.hashCode();
  }

  /** Returns the field name for this query */
  public final String getField() { return query.getField(); }

  /**
   * Returns a DocIdSet with documents that should be permitted in search
   * results.
   */
  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
    final Terms terms = context.reader().terms(query.field);
    if (terms == null) {
      // field does not exist
      return null;
    }

    final TermsEnum termsEnum = query.getTermsEnum(terms);
    assert termsEnum != null;

    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(context.reader().maxDoc());
    PostingsEnum docs = null;
    while (termsEnum.next() != null) {
      docs = termsEnum.postings(docs, PostingsEnum.NONE);
      builder.or(docs);
    }
    return BitsFilteredDocIdSet.wrap(builder.build(), acceptDocs);
  }
}
