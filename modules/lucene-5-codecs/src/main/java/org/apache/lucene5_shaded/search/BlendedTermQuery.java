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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.IndexReaderContext;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.search.BooleanClause.Occur;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.InPlaceMergeSorter;
import org.apache.lucene5_shaded.util.ToStringUtils;

/**
 * A {@link Query} that blends index statistics across multiple terms.
 * This is particularly useful when several terms should produce identical
 * scores, regardless of their index statistics.
 * <p>For instance imagine that you are resolving synonyms at search time,
 * all terms should produce identical scores instead of the default behavior,
 * which tends to give higher scores to rare terms.
 * <p>An other useful use-case is cross-field search: imagine that you would
 * like to search for {@code john} on two fields: {@code first_name} and
 * {@code last_name}. You might not want to give a higher weight to matches
 * on the field where {@code john} is rarer, in which case
 * {@link BlendedTermQuery} would help as well.
 * @lucene.experimental
 */
public final class BlendedTermQuery extends Query {

  /** A Builder for {@link BlendedTermQuery}. */
  public static class Builder {

    private int numTerms = 0;
    private Term[] terms = new Term[0];
    private float[] boosts = new float[0];
    private TermContext[] contexts = new TermContext[0];
    private RewriteMethod rewriteMethod = DISJUNCTION_MAX_REWRITE;

    /** Sole constructor. */
    public Builder() {}

    /** Set the {@link RewriteMethod}. Default is to use
     *  {@link BlendedTermQuery#DISJUNCTION_MAX_REWRITE}.
     *  @see RewriteMethod */
    public Builder setRewriteMethod(RewriteMethod rewiteMethod) {
      this.rewriteMethod = rewiteMethod;
      return this;
    }

    /** Add a new {@link Term} to this builder, with a default boost of {@code 1}.
     *  @see #add(Term, float) */
    public Builder add(Term term) {
      return add(term, 1f);
    }

    /** Add a {@link Term} with the provided boost. The higher the boost, the
     *  more this term will contribute to the overall score of the
     *  {@link BlendedTermQuery}. */
    public Builder add(Term term, float boost) {
      return add(term, boost, null);
    }

    /**
     * Expert: Add a {@link Term} with the provided boost and context.
     * This method is useful if you already have a {@link TermContext}
     * object constructed for the given term.
     */
    public Builder add(Term term, float boost, TermContext context) {
      if (numTerms >= BooleanQuery.getMaxClauseCount()) {
        throw new BooleanQuery.TooManyClauses();
      }
      terms = ArrayUtil.grow(terms, numTerms + 1);
      boosts = ArrayUtil.grow(boosts, numTerms + 1);
      contexts = ArrayUtil.grow(contexts, numTerms + 1);
      terms[numTerms] = term;
      boosts[numTerms] = boost;
      contexts[numTerms] = context;
      numTerms += 1;
      return this;
    }

    /** Build the {@link BlendedTermQuery}. */
    public BlendedTermQuery build() {
      return new BlendedTermQuery(
          Arrays.copyOf(terms, numTerms),
          Arrays.copyOf(boosts, numTerms),
          Arrays.copyOf(contexts, numTerms),
          rewriteMethod);
    }

  }

  /** A {@link RewriteMethod} defines how queries for individual terms should
   *  be merged.
   *  @lucene.experimental
   *  @see BlendedTermQuery#BOOLEAN_REWRITE
   *  @see DisjunctionMaxRewrite */
  public static abstract class RewriteMethod {

    /** Sole constructor */
    protected RewriteMethod() {}

    /** Merge the provided sub queries into a single {@link Query} object. */
    public abstract Query rewrite(Query[] subQueries);

  }

  /**
   * A {@link RewriteMethod} that adds all sub queries to a {@link BooleanQuery}
   * which has {@link BooleanQuery#isCoordDisabled() coords disabled}. This
   * {@link RewriteMethod} is useful when matching on several fields is
   * considered better than having a good match on a single field.
   */
  public static final RewriteMethod BOOLEAN_REWRITE = new RewriteMethod() {
    @Override
    public Query rewrite(Query[] subQueries) {
      BooleanQuery.Builder merged = new BooleanQuery.Builder();
      merged.setDisableCoord(true);
      for (Query query : subQueries) {
        merged.add(query, Occur.SHOULD);
      }
      return merged.build();
    }
  };

  /**
   * A {@link RewriteMethod} that creates a {@link DisjunctionMaxQuery} out
   * of the sub queries. This {@link RewriteMethod} is useful when having a
   * good match on a single field is considered better than having average
   * matches on several fields.
   */
  public static class DisjunctionMaxRewrite extends RewriteMethod {

    private final float tieBreakerMultiplier;

    /** This {@link RewriteMethod} will create {@link DisjunctionMaxQuery}
     *  instances that have the provided tie breaker.
     *  @see DisjunctionMaxQuery */
    public DisjunctionMaxRewrite(float tieBreakerMultiplier) {
      this.tieBreakerMultiplier = tieBreakerMultiplier;
    }

    @Override
    public Query rewrite(Query[] subQueries) {
      return new DisjunctionMaxQuery(Arrays.asList(subQueries), tieBreakerMultiplier);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      DisjunctionMaxRewrite that = (DisjunctionMaxRewrite) obj;
      return tieBreakerMultiplier == that.tieBreakerMultiplier;
    }

    @Override
    public int hashCode() {
      return 31 * getClass().hashCode() + Float.floatToIntBits(tieBreakerMultiplier);
    }

  }

  /** {@link DisjunctionMaxRewrite} instance with a tie-breaker of {@code 0.01}. */
  public static final RewriteMethod DISJUNCTION_MAX_REWRITE = new DisjunctionMaxRewrite(0.01f);

  private final Term[] terms;
  private final float[] boosts;
  private final TermContext[] contexts;
  private final RewriteMethod rewriteMethod;

  private BlendedTermQuery(final Term[] terms, final float[] boosts, final TermContext[] contexts,
      RewriteMethod rewriteMethod) {
    assert terms.length == boosts.length;
    assert terms.length == contexts.length;
    this.terms = terms;
    this.boosts = boosts;
    this.contexts = contexts;
    this.rewriteMethod = rewriteMethod;

    // we sort terms so that equals/hashcode does not rely on the order
    new InPlaceMergeSorter() {

      @Override
      protected void swap(int i, int j) {
        Term tmpTerm = terms[i];
        terms[i] = terms[j];
        terms[j] = tmpTerm;

        TermContext tmpContext = contexts[i];
        contexts[i] = contexts[j];
        contexts[j] = tmpContext;

        float tmpBoost = boosts[i];
        boosts[i] = boosts[j];
        boosts[j] = tmpBoost;
      }

      @Override
      protected int compare(int i, int j) {
        return terms[i].compareTo(terms[j]);
      }
    }.sort(0, terms.length);
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj) == false) {
      return false;
    }
    BlendedTermQuery that = (BlendedTermQuery) obj;
    return Arrays.equals(terms, that.terms)
        && Arrays.equals(contexts, that.contexts)
        && Arrays.equals(boosts, that.boosts)
        && rewriteMethod.equals(that.rewriteMethod);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = 31 * h + Arrays.hashCode(terms);
    h = 31 * h + Arrays.hashCode(contexts);
    h = 31 * h + Arrays.hashCode(boosts);
    h = 31 * h + rewriteMethod.hashCode();
    return h;
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder("Blended(");
    for (int i = 0; i < terms.length; ++i) {
      if (i != 0) {
        builder.append(" ");
      }
      Query termQuery = new TermQuery(terms[i]);
      if (boosts[i] != 1f) {
        termQuery = new BoostQuery(termQuery, boosts[i]);
      }
      builder.append(termQuery.toString(field));
    }
    builder.append(")");
    builder.append(ToStringUtils.boost(getBoost()));
    return builder.toString();
  }

  @Override
  public final Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    final TermContext[] contexts = Arrays.copyOf(this.contexts, this.contexts.length);
    for (int i = 0; i < contexts.length; ++i) {
      if (contexts[i] == null || contexts[i].wasBuiltFor(reader.getContext()) == false) {
        contexts[i] = TermContext.build(reader.getContext(), terms[i]);
      }
    }

    // Compute aggregated doc freq and total term freq
    // df will be the max of all doc freqs
    // ttf will be the sum of all total term freqs
    int df = 0;
    long ttf = 0;
    for (TermContext ctx : contexts) {
      df = Math.max(df, ctx.docFreq());
      if (ctx.totalTermFreq() == -1L) {
        ttf = -1L;
      } else if (ttf != -1L) {
        ttf += ctx.totalTermFreq();
      }
    }

    for (int i = 0; i < contexts.length; ++i) {
      contexts[i] = adjustFrequencies(reader.getContext(), contexts[i], df, ttf);
    }

    Query[] termQueries = new Query[terms.length];
    for (int i = 0; i < terms.length; ++i) {
      termQueries[i] = new TermQuery(terms[i], contexts[i]);
      if (boosts[i] != 1f) {
        termQueries[i] = new BoostQuery(termQueries[i], boosts[i]);
      }
    }
    return rewriteMethod.rewrite(termQueries);
  }

  private static TermContext adjustFrequencies(IndexReaderContext readerContext,
      TermContext ctx, int artificialDf, long artificialTtf) {
    List<LeafReaderContext> leaves = readerContext.leaves();
    final int len;
    if (leaves == null) {
      len = 1;
    } else {
      len = leaves.size();
    }
    TermContext newCtx = new TermContext(readerContext);
    for (int i = 0; i < len; ++i) {
      TermState termState = ctx.get(i);
      if (termState == null) {
        continue;
      }
      newCtx.register(termState, i);
    }
    newCtx.accumulateStatistics(artificialDf, artificialTtf);
    return newCtx;
  }

}
