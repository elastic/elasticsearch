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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.TermContext;
import org.apache.lucene5_shaded.index.TermState;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;

/**
 * Base rewrite method for collecting only the top terms
 * via a priority queue.
 * @lucene.internal Only public to be accessible by spans package.
 */
public abstract class TopTermsRewrite<B> extends TermCollectingRewrite<B> {

  private final int size;
  
  /** 
   * Create a TopTermsBooleanQueryRewrite for 
   * at most <code>size</code> terms.
   * <p>
   * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than 
   * <code>size</code>, then it will be used instead. 
   */
  public TopTermsRewrite(int size) {
    this.size = size;
  }
  
  /** return the maximum priority queue size */
  public int getSize() {
    return size;
  }
  
  /** return the maximum size of the priority queue (for boolean rewrites this is BooleanQuery#getMaxClauseCount). */
  protected abstract int getMaxSize();
  
  @Override
  public final Query rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {
    final int maxSize = Math.min(size, getMaxSize());
    final PriorityQueue<ScoreTerm> stQueue = new PriorityQueue<>();
    collectTerms(reader, query, new TermCollector() {
      private final MaxNonCompetitiveBoostAttribute maxBoostAtt =
        attributes.addAttribute(MaxNonCompetitiveBoostAttribute.class);
      
      private final Map<BytesRef,ScoreTerm> visitedTerms = new HashMap<>();
      
      private TermsEnum termsEnum;
      private BoostAttribute boostAtt;        
      private ScoreTerm st;
      
      @Override
      public void setNextEnum(TermsEnum termsEnum) {
        this.termsEnum = termsEnum;
        
        assert compareToLastTerm(null);

        // lazy init the initial ScoreTerm because comparator is not known on ctor:
        if (st == null)
          st = new ScoreTerm(new TermContext(topReaderContext));
        boostAtt = termsEnum.attributes().addAttribute(BoostAttribute.class);
      }
    
      // for assert:
      private BytesRefBuilder lastTerm;
      private boolean compareToLastTerm(BytesRef t) {
        if (lastTerm == null && t != null) {
          lastTerm = new BytesRefBuilder();
          lastTerm.append(t);
        } else if (t == null) {
          lastTerm = null;
        } else {
          assert lastTerm.get().compareTo(t) < 0: "lastTerm=" + lastTerm + " t=" + t;
          lastTerm.copyBytes(t);
        }
        return true;
      }
  
      @Override
      public boolean collect(BytesRef bytes) throws IOException {
        final float boost = boostAtt.getBoost();

        // make sure within a single seg we always collect
        // terms in order
        assert compareToLastTerm(bytes);

        //System.out.println("TTR.collect term=" + bytes.utf8ToString() + " boost=" + boost + " ord=" + readerContext.ord);
        // ignore uncompetitive hits
        if (stQueue.size() == maxSize) {
          final ScoreTerm t = stQueue.peek();
          if (boost < t.boost)
            return true;
          if (boost == t.boost && bytes.compareTo(t.bytes.get()) > 0)
            return true;
        }
        ScoreTerm t = visitedTerms.get(bytes);
        final TermState state = termsEnum.termState();
        assert state != null;
        if (t != null) {
          // if the term is already in the PQ, only update docFreq of term in PQ
          assert t.boost == boost : "boost should be equal in all segment TermsEnums";
          t.termState.register(state, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
        } else {
          // add new entry in PQ, we must clone the term, else it may get overwritten!
          st.bytes.copyBytes(bytes);
          st.boost = boost;
          visitedTerms.put(st.bytes.get(), st);
          assert st.termState.docFreq() == 0;
          st.termState.register(state, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
          stQueue.offer(st);
          // possibly drop entries from queue
          if (stQueue.size() > maxSize) {
            st = stQueue.poll();
            visitedTerms.remove(st.bytes.get());
            st.termState.clear(); // reset the termstate! 
          } else {
            st = new ScoreTerm(new TermContext(topReaderContext));
          }
          assert stQueue.size() <= maxSize : "the PQ size must be limited to maxSize";
          // set maxBoostAtt with values to help FuzzyTermsEnum to optimize
          if (stQueue.size() == maxSize) {
            t = stQueue.peek();
            maxBoostAtt.setMaxNonCompetitiveBoost(t.boost);
            maxBoostAtt.setCompetitiveTerm(t.bytes.get());
          }
        }
       
        return true;
      }
    });
    
    final B b = getTopLevelBuilder();
    final ScoreTerm[] scoreTerms = stQueue.toArray(new ScoreTerm[stQueue.size()]);
    ArrayUtil.timSort(scoreTerms, scoreTermSortByTermComp);

    for (final ScoreTerm st : scoreTerms) {
      final Term term = new Term(query.field, st.bytes.toBytesRef());
      addClause(b, term, st.termState.docFreq(), st.boost, st.termState); // add to query
    }
    return build(b);
  }

  @Override
  public int hashCode() {
    return 31 * size;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    final TopTermsRewrite<?> other = (TopTermsRewrite<?>) obj;
    if (size != other.size) return false;
    return true;
  }
  
  private static final Comparator<ScoreTerm> scoreTermSortByTermComp = 
    new Comparator<ScoreTerm>() {
      @Override
      public int compare(ScoreTerm st1, ScoreTerm st2) {
        return st1.bytes.get().compareTo(st2.bytes.get());
      }
    };

  static final class ScoreTerm implements Comparable<ScoreTerm> {
    public final BytesRefBuilder bytes = new BytesRefBuilder();
    public float boost;
    public final TermContext termState;
    public ScoreTerm(TermContext termState) {
      this.termState = termState;
    }
    
    @Override
    public int compareTo(ScoreTerm other) {
      if (this.boost == other.boost)
        return other.bytes.get().compareTo(this.bytes.get());
      else
        return Float.compare(this.boost, other.boost);
    }
  }
}
