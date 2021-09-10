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
import java.util.Objects;

import org.apache.lucene5_shaded.index.FilteredTermsEnum; // javadocs
import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.SingleTermsEnum;   // javadocs
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.AttributeSource;

/**
 * An abstract {@link Query} that matches documents
 * containing a subset of terms provided by a {@link
 * FilteredTermsEnum} enumeration.
 *
 * <p>This query cannot be used directly; you must subclass
 * it and define {@link #getTermsEnum(Terms,AttributeSource)} to provide a {@link
 * FilteredTermsEnum} that iterates through the terms to be
 * matched.
 *
 * <p><b>NOTE</b>: if {@link #setRewriteMethod} is either
 * {@link #CONSTANT_SCORE_BOOLEAN_REWRITE} or {@link
 * #SCORING_BOOLEAN_REWRITE}, you may encounter a
 * {@link BooleanQuery.TooManyClauses} exception during
 * searching, which happens when the number of terms to be
 * searched exceeds {@link
 * BooleanQuery#getMaxClauseCount()}.  Setting {@link
 * #setRewriteMethod} to {@link #CONSTANT_SCORE_REWRITE}
 * prevents this.
 *
 * <p>The recommended rewrite method is {@link
 * #CONSTANT_SCORE_REWRITE}: it doesn't spend CPU
 * computing unhelpful scores, and is the most
 * performant rewrite method given the query. If you
 * need scoring (like {@link FuzzyQuery}, use
 * {@link TopTermsScoringBooleanQueryRewrite} which uses
 * a priority queue to only collect competitive terms
 * and not hit this limitation.
 *
 * Note that org.apache.lucene5_shaded.queryparser.classic.QueryParser produces
 * MultiTermQueries using {@link #CONSTANT_SCORE_REWRITE}
 * by default.
 */
public abstract class MultiTermQuery extends Query {
  protected final String field;
  protected RewriteMethod rewriteMethod = CONSTANT_SCORE_REWRITE;

  /** Abstract class that defines how the query is rewritten. */
  public static abstract class RewriteMethod {
    public abstract Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException;
    /**
     * Returns the {@link MultiTermQuery}s {@link TermsEnum}
     * @see MultiTermQuery#getTermsEnum(Terms, AttributeSource)
     */
    protected TermsEnum getTermsEnum(MultiTermQuery query, Terms terms, AttributeSource atts) throws IOException {
      return query.getTermsEnum(terms, atts); // allow RewriteMethod subclasses to pull a TermsEnum from the MTQ 
    }
  }

  /** A rewrite method that first creates a private Filter,
   *  by visiting each term in sequence and marking all docs
   *  for that term.  Matching documents are assigned a
   *  constant score equal to the query's boost.
   * 
   *  <p> This method is faster than the BooleanQuery
   *  rewrite methods when the number of matched terms or
   *  matched documents is non-trivial. Also, it will never
   *  hit an errant {@link BooleanQuery.TooManyClauses}
   *  exception.
   *
   *  @see #setRewriteMethod */
  public static final RewriteMethod CONSTANT_SCORE_REWRITE = new RewriteMethod() {
    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) {
      return new MultiTermQueryConstantScoreWrapper<>(query);
    }
  };

  /** Old name of {@link #CONSTANT_SCORE_REWRITE}
   *  @deprecated old name of {@link #CONSTANT_SCORE_REWRITE} */
  @Deprecated
  public static final RewriteMethod CONSTANT_SCORE_FILTER_REWRITE = CONSTANT_SCORE_REWRITE;

  /** A rewrite method that first translates each term into
   *  {@link BooleanClause.Occur#SHOULD} clause in a
   *  BooleanQuery, and keeps the scores as computed by the
   *  query.  Note that typically such scores are
   *  meaningless to the user, and require non-trivial CPU
   *  to compute, so it's almost always better to use {@link
   *  #CONSTANT_SCORE_REWRITE} instead.
   *
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see #setRewriteMethod */
  public final static RewriteMethod SCORING_BOOLEAN_REWRITE = ScoringRewrite.SCORING_BOOLEAN_REWRITE;
  
  /** Old name of {@link #SCORING_BOOLEAN_REWRITE}
   *  @deprecated old name of {@link #SCORING_BOOLEAN_REWRITE} */
  @Deprecated
  public final static RewriteMethod SCORING_BOOLEAN_QUERY_REWRITE = SCORING_BOOLEAN_REWRITE;
  
  /** Like {@link #SCORING_BOOLEAN_REWRITE} except
   *  scores are not computed.  Instead, each matching
   *  document receives a constant score equal to the
   *  query's boost.
   * 
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see #setRewriteMethod */
  public final static RewriteMethod CONSTANT_SCORE_BOOLEAN_REWRITE = ScoringRewrite.CONSTANT_SCORE_BOOLEAN_REWRITE;

  /** Old name of {@link #CONSTANT_SCORE_BOOLEAN_REWRITE}
   *  @deprecated old name of {@link #CONSTANT_SCORE_BOOLEAN_REWRITE} */
  @Deprecated
  public final static RewriteMethod CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE = CONSTANT_SCORE_BOOLEAN_REWRITE;

  /**
   * A rewrite method that first translates each term into
   * {@link BooleanClause.Occur#SHOULD} clause in a BooleanQuery, and keeps the
   * scores as computed by the query.
   * 
   * <p>
   * This rewrite method only uses the top scoring terms so it will not overflow
   * the boolean max clause count. It is the default rewrite method for
   * {@link FuzzyQuery}.
   * 
   * @see #setRewriteMethod
   */
  public static final class TopTermsScoringBooleanQueryRewrite extends TopTermsRewrite<BooleanQuery.Builder> {

    /** 
     * Create a TopTermsScoringBooleanQueryRewrite for 
     * at most <code>size</code> terms.
     * <p>
     * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than 
     * <code>size</code>, then it will be used instead. 
     */
    public TopTermsScoringBooleanQueryRewrite(int size) {
      super(size);
    }
    
    @Override
    protected int getMaxSize() {
      return BooleanQuery.getMaxClauseCount();
    }
    
    @Override
    protected BooleanQuery.Builder getTopLevelBuilder() {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.setDisableCoord(true);
      return builder;
    }

    @Override
    protected Query build(BooleanQuery.Builder builder) {
      return builder.build();
    }
    
    @Override
    protected void addClause(BooleanQuery.Builder topLevel, Term term, int docCount, float boost, TermContext states) {
      final TermQuery tq = new TermQuery(term, states);
      topLevel.add(new BoostQuery(tq, boost), BooleanClause.Occur.SHOULD);
    }
  }
  
  /**
   * A rewrite method that first translates each term into
   * {@link BooleanClause.Occur#SHOULD} clause in a BooleanQuery, but adjusts
   * the frequencies used for scoring to be blended across the terms, otherwise
   * the rarest term typically ranks highest (often not useful eg in the set of
   * expanded terms in a FuzzyQuery).
   * 
   * <p>
   * This rewrite method only uses the top scoring terms so it will not overflow
   * the boolean max clause count.
   * 
   * @see #setRewriteMethod
   */
  public static final class TopTermsBlendedFreqScoringRewrite extends
      TopTermsRewrite<BlendedTermQuery.Builder> {

    /**
     * Create a TopTermsBlendedScoringBooleanQueryRewrite for at most
     * <code>size</code> terms.
     * <p>
     * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than
     * <code>size</code>, then it will be used instead.
     */
    public TopTermsBlendedFreqScoringRewrite(int size) {
      super(size);
    }

    @Override
    protected int getMaxSize() {
      return BooleanQuery.getMaxClauseCount();
    }

    @Override
    protected BlendedTermQuery.Builder getTopLevelBuilder() {
      BlendedTermQuery.Builder builder = new BlendedTermQuery.Builder();
      builder.setRewriteMethod(BlendedTermQuery.BOOLEAN_REWRITE);
      return builder;
    }

    @Override
    protected Query build(BlendedTermQuery.Builder builder) {
      return builder.build();
    }

    @Override
    protected void addClause(BlendedTermQuery.Builder topLevel, Term term, int docCount,
        float boost, TermContext states) {
      topLevel.add(term, boost, states);
    }
  }

  /**
   * A rewrite method that first translates each term into
   * {@link BooleanClause.Occur#SHOULD} clause in a BooleanQuery, but the scores
   * are only computed as the boost.
   * <p>
   * This rewrite method only uses the top scoring terms so it will not overflow
   * the boolean max clause count.
   * 
   * @see #setRewriteMethod
   */
  public static final class TopTermsBoostOnlyBooleanQueryRewrite extends TopTermsRewrite<BooleanQuery.Builder> {
    
    /** 
     * Create a TopTermsBoostOnlyBooleanQueryRewrite for 
     * at most <code>size</code> terms.
     * <p>
     * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than 
     * <code>size</code>, then it will be used instead. 
     */
    public TopTermsBoostOnlyBooleanQueryRewrite(int size) {
      super(size);
    }
    
    @Override
    protected int getMaxSize() {
      return BooleanQuery.getMaxClauseCount();
    }
    
    @Override
    protected BooleanQuery.Builder getTopLevelBuilder() {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.setDisableCoord(true);
      return builder;
    }
    
    @Override
    protected Query build(BooleanQuery.Builder builder) {
      return builder.build();
    }
    
    @Override
    protected void addClause(BooleanQuery.Builder topLevel, Term term, int docFreq, float boost, TermContext states) {
      final Query q = new ConstantScoreQuery(new TermQuery(term, states));
      topLevel.add(new BoostQuery(q, boost), BooleanClause.Occur.SHOULD);
    }
  }

  /**
   * Constructs a query matching terms that cannot be represented with a single
   * Term.
   */
  public MultiTermQuery(final String field) {
    this.field = Objects.requireNonNull(field, "field must not be null");
  }

  /** Returns the field name for this query */
  public final String getField() { return field; }

  /** Construct the enumeration to be used, expanding the
   *  pattern term.  This method should only be called if
   *  the field exists (ie, implementations can assume the
   *  field does exist).  This method should not return null
   *  (should instead return {@link TermsEnum#EMPTY} if no
   *  terms match).  The TermsEnum must already be
   *  positioned to the first matching term.
   * The given {@link AttributeSource} is passed by the {@link RewriteMethod} to
   * provide attributes, the rewrite method uses to inform about e.g. maximum competitive boosts.
   * This is currently only used by {@link TopTermsRewrite}
   */
  protected abstract TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException;

  /** Convenience method, if no attributes are needed:
   * This simply passes empty attributes and is equal to:
   * <code>getTermsEnum(terms, new AttributeSource())</code>
   */
  protected final TermsEnum getTermsEnum(Terms terms) throws IOException {
    return getTermsEnum(terms, new AttributeSource());
  }

  /**
   * To rewrite to a simpler form, instead return a simpler
   * enum from {@link #getTermsEnum(Terms, AttributeSource)}.  For example,
   * to rewrite to a single term, return a {@link SingleTermsEnum}
   */
  @Override
  public final Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    return rewriteMethod.rewrite(reader, this);
  }

  /**
   * @see #setRewriteMethod
   */
  public RewriteMethod getRewriteMethod() {
    return rewriteMethod;
  }

  /**
   * Sets the rewrite method to be used when executing the
   * query.  You can use one of the four core methods, or
   * implement your own subclass of {@link RewriteMethod}. */
  public void setRewriteMethod(RewriteMethod method) {
    rewriteMethod = method;
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = 31 * h + rewriteMethod.hashCode();
    h = 31 * h + Objects.hashCode(field);
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MultiTermQuery other = (MultiTermQuery) obj;
    if (!super.equals(obj))
      return false;
    if (!rewriteMethod.equals(other.rewriteMethod)) {
      return false;
    }
    return (other.field == null ? field == null : other.field.equals(field));
  }
 
}
