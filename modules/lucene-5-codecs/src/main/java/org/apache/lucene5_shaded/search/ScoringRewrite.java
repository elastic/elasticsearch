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
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.ByteBlockPool;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene5_shaded.util.BytesRefHash;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/** 
 * Base rewrite method that translates each term into a query, and keeps
 * the scores as computed by the query.
 * <p>
 * @lucene.internal Only public to be accessible by spans package. */
public abstract class ScoringRewrite<B> extends TermCollectingRewrite<B> {

  /** A rewrite method that first translates each term into
   *  {@link BooleanClause.Occur#SHOULD} clause in a
   *  BooleanQuery, and keeps the scores as computed by the
   *  query.  Note that typically such scores are
   *  meaningless to the user, and require non-trivial CPU
   *  to compute, so it's almost always better to use {@link
   *  MultiTermQuery#CONSTANT_SCORE_REWRITE} instead.
   *
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see MultiTermQuery#setRewriteMethod */
  public final static ScoringRewrite<BooleanQuery.Builder> SCORING_BOOLEAN_REWRITE = new ScoringRewrite<BooleanQuery.Builder>() {
    @Override
    protected BooleanQuery.Builder getTopLevelBuilder() {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.setDisableCoord(true);
      return builder;
    }
    
    protected Query build(BooleanQuery.Builder builder) {
      return builder.build();
    }
    
    @Override
    protected void addClause(BooleanQuery.Builder topLevel, Term term, int docCount,
        float boost, TermContext states) {
      final TermQuery tq = new TermQuery(term, states);
      topLevel.add(new BoostQuery(tq, boost), BooleanClause.Occur.SHOULD);
    }
    
    @Override
    protected void checkMaxClauseCount(int count) {
      if (count > BooleanQuery.getMaxClauseCount())
        throw new BooleanQuery.TooManyClauses();
    }
  };
  
  /** Like {@link #SCORING_BOOLEAN_REWRITE} except
   *  scores are not computed.  Instead, each matching
   *  document receives a constant score equal to the
   *  query's boost.
   * 
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see MultiTermQuery#setRewriteMethod */
  public final static RewriteMethod CONSTANT_SCORE_BOOLEAN_REWRITE = new RewriteMethod() {
    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
      final Query bq = SCORING_BOOLEAN_REWRITE.rewrite(reader, query);
      // strip the scores off
      return new ConstantScoreQuery(bq);
    }
  };

  /** This method is called after every new term to check if the number of max clauses
   * (e.g. in BooleanQuery) is not exceeded. Throws the corresponding {@link RuntimeException}. */
  protected abstract void checkMaxClauseCount(int count) throws IOException;
  
  @Override
  public final Query rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {
    final B builder = getTopLevelBuilder();
    final ParallelArraysTermCollector col = new ParallelArraysTermCollector();
    collectTerms(reader, query, col);
    
    final int size = col.terms.size();
    if (size > 0) {
      final int sort[] = col.terms.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
      final float[] boost = col.array.boost;
      final TermContext[] termStates = col.array.termState;
      for (int i = 0; i < size; i++) {
        final int pos = sort[i];
        final Term term = new Term(query.getField(), col.terms.get(pos, new BytesRef()));
        assert termStates[pos].hasOnlyRealTerms() == false || reader.docFreq(term) == termStates[pos].docFreq();
        addClause(builder, term, termStates[pos].docFreq(), boost[pos], termStates[pos]);
      }
    }
    return build(builder);
  }

  final class ParallelArraysTermCollector extends TermCollector {
    final TermFreqBoostByteStart array = new TermFreqBoostByteStart(16);
    final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectAllocator()), 16, array);
    TermsEnum termsEnum;

    private BoostAttribute boostAtt;
    
    @Override
    public void setNextEnum(TermsEnum termsEnum) {
      this.termsEnum = termsEnum;
      this.boostAtt = termsEnum.attributes().addAttribute(BoostAttribute.class);
    }
  
    @Override
    public boolean collect(BytesRef bytes) throws IOException {
      final int e = terms.add(bytes);
      final TermState state = termsEnum.termState();
      assert state != null; 
      if (e < 0) {
        // duplicate term: update docFreq
        final int pos = (-e)-1;
        array.termState[pos].register(state, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
        assert array.boost[pos] == boostAtt.getBoost() : "boost should be equal in all segment TermsEnums";
      } else {
        // new entry: we populate the entry initially
        array.boost[e] = boostAtt.getBoost();
        array.termState[e] = new TermContext(topReaderContext, state, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
        ScoringRewrite.this.checkMaxClauseCount(terms.size());
      }
      return true;
    }
  }
  
  /** Special implementation of BytesStartArray that keeps parallel arrays for boost and docFreq */
  static final class TermFreqBoostByteStart extends DirectBytesStartArray  {
    float[] boost;
    TermContext[] termState;
    
    public TermFreqBoostByteStart(int initSize) {
      super(initSize);
    }

    @Override
    public int[] init() {
      final int[] ord = super.init();
      boost = new float[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_FLOAT)];
      termState = new TermContext[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      assert termState.length >= ord.length && boost.length >= ord.length;
      return ord;
    }

    @Override
    public int[] grow() {
      final int[] ord = super.grow();
      boost = ArrayUtil.grow(boost, ord.length);
      if (termState.length < ord.length) {
        TermContext[] tmpTermState = new TermContext[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(termState, 0, tmpTermState, 0, termState.length);
        termState = tmpTermState;
      }     
      assert termState.length >= ord.length && boost.length >= ord.length;
      return ord;
    }

    @Override
    public int[] clear() {
     boost = null;
     termState = null;
     return super.clear();
    }
    
  }
}
