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
package org.apache.lucene5_shaded.search.spans;


import java.io.IOException;
import java.util.Objects;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.search.BooleanClause.Occur;
import org.apache.lucene5_shaded.search.IndexSearcher;
import org.apache.lucene5_shaded.search.MultiTermQuery;
import org.apache.lucene5_shaded.search.Query;
import org.apache.lucene5_shaded.search.ScoringRewrite;
import org.apache.lucene5_shaded.search.TopTermsRewrite;

/**
 * Wraps any {@link MultiTermQuery} as a {@link SpanQuery}, 
 * so it can be nested within other SpanQuery classes.
 * <p>
 * The query is rewritten by default to a {@link SpanOrQuery} containing
 * the expanded terms, but this can be customized. 
 * <p>
 * Example:
 * <blockquote><pre class="prettyprint">
 * {@code
 * WildcardQuery wildcard = new WildcardQuery(new Term("field", "bro?n"));
 * SpanQuery spanWildcard = new SpanMultiTermQueryWrapper<WildcardQuery>(wildcard);
 * // do something with spanWildcard, such as use it in a SpanFirstQuery
 * }
 * </pre></blockquote>
 */
public class SpanMultiTermQueryWrapper<Q extends MultiTermQuery> extends SpanQuery {

  protected final Q query;
  private SpanRewriteMethod rewriteMethod;

  /**
   * Create a new SpanMultiTermQueryWrapper. 
   * 
   * @param query Query to wrap.
   */
  @SuppressWarnings({"rawtypes","unchecked"})
  public SpanMultiTermQueryWrapper(Q query) {
    this.query = Objects.requireNonNull(query);
    this.rewriteMethod = selectRewriteMethod(query);
  }

  private static SpanRewriteMethod selectRewriteMethod(MultiTermQuery query) {
    MultiTermQuery.RewriteMethod method = query.getRewriteMethod();
    if (method instanceof TopTermsRewrite) {
      final int pqsize = ((TopTermsRewrite) method).getSize();
      return new TopTermsSpanBooleanQueryRewrite(pqsize);
    } else {
      return SCORING_SPAN_QUERY_REWRITE;
    }
  }

  /**
   * Expert: returns the rewriteMethod
   */
  public final SpanRewriteMethod getRewriteMethod() {
    return rewriteMethod;
  }

  /**
   * Expert: sets the rewrite method. This only makes sense
   * to be a span rewrite method.
   */
  public final void setRewriteMethod(SpanRewriteMethod rewriteMethod) {
    this.rewriteMethod = rewriteMethod;
  }

  @Override
  public String getField() {
    return query.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    throw new IllegalArgumentException("Rewrite first!");
  }

  /** Returns the wrapped query */
  public Query getWrappedQuery() {
    return query;
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder();
    builder.append("SpanMultiTermQueryWrapper(");
    builder.append(query.toString(field));
    builder.append(")");
    return builder.toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    return rewriteMethod.rewrite(reader, query);
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + query.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (! super.equals(obj)) {
      return false;
    }
    SpanMultiTermQueryWrapper<?> other = (SpanMultiTermQueryWrapper<?>) obj;
    return query.equals(other.query);
  }

  /** Abstract class that defines how the query is rewritten. */
  public static abstract class SpanRewriteMethod extends MultiTermQuery.RewriteMethod {
    @Override
    public abstract SpanQuery rewrite(IndexReader reader, MultiTermQuery query) throws IOException;
  }

  /**
   * A rewrite method that first translates each term into a SpanTermQuery in a
   * {@link Occur#SHOULD} clause in a BooleanQuery, and keeps the
   * scores as computed by the query.
   * 
   * @see #setRewriteMethod
   */
  public final static SpanRewriteMethod SCORING_SPAN_QUERY_REWRITE = new SpanRewriteMethod() {
    private final ScoringRewrite<List<SpanQuery>> delegate = new ScoringRewrite<List<SpanQuery>>() {
      @Override
      protected List<SpanQuery> getTopLevelBuilder() {
        return new ArrayList<SpanQuery>();
      }

      protected Query build(List<SpanQuery> builder) {
        return new SpanOrQuery(builder.toArray(new SpanQuery[builder.size()]));
      }

      @Override
      protected void checkMaxClauseCount(int count) {
        // we accept all terms as SpanOrQuery has no limits
      }
    
      @Override
      protected void addClause(List<SpanQuery> topLevel, Term term, int docCount, float boost, TermContext states) {
        final SpanTermQuery q = new SpanTermQuery(term, states);
        topLevel.add(q);
      }
    };
    
    @Override
    public SpanQuery rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
      return (SpanQuery) delegate.rewrite(reader, query);
    }
  };
  
  /**
   * A rewrite method that first translates each term into a SpanTermQuery in a
   * {@link Occur#SHOULD} clause in a BooleanQuery, and keeps the
   * scores as computed by the query.
   * 
   * <p>
   * This rewrite method only uses the top scoring terms so it will not overflow
   * the boolean max clause count.
   * 
   * @see #setRewriteMethod
   */
  public static final class TopTermsSpanBooleanQueryRewrite extends SpanRewriteMethod  {
    private final TopTermsRewrite<List<SpanQuery>> delegate;
  
    /** 
     * Create a TopTermsSpanBooleanQueryRewrite for 
     * at most <code>size</code> terms.
     */
    public TopTermsSpanBooleanQueryRewrite(int size) {
      delegate = new TopTermsRewrite<List<SpanQuery>>(size) {
        @Override
        protected int getMaxSize() {
          return Integer.MAX_VALUE;
        }
    
        @Override
        protected List<SpanQuery> getTopLevelBuilder() {
          return new ArrayList<SpanQuery>();
        }

        @Override
        protected Query build(List<SpanQuery> builder) {
          return new SpanOrQuery(builder.toArray(new SpanQuery[builder.size()]));
        }

        @Override
        protected void addClause(List<SpanQuery> topLevel, Term term, int docFreq, float boost, TermContext states) {
          final SpanTermQuery q = new SpanTermQuery(term, states);
          topLevel.add(q);
        }
      };
    }
    
    /** return the maximum priority queue size */
    public int getSize() {
      return delegate.getSize();
    }

    @Override
    public SpanQuery rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
      return (SpanQuery) delegate.rewrite(reader, query);
    }
  
    @Override
    public int hashCode() {
      return 31 * delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      final TopTermsSpanBooleanQueryRewrite other = (TopTermsSpanBooleanQueryRewrite) obj;
      return delegate.equals(other.delegate);
    }
    
  }
  
}
