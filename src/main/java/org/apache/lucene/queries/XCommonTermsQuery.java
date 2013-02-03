package org.apache.lucene.queries;

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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ToStringUtils;

/**
 * A query that executes high-frequency terms in a optional sub-query to prevent
 * slow queries due to "common" terms like stopwords. This query basically
 * builds 2 queries off the {@link #add(Term) added} terms where low-frequency
 * terms are added to a required boolean clause and high-frequency terms are
 * added to an optional boolean clause. The optional clause is only executed if
 * the required "low-frequency' clause matches. Scores produced by this query
 * will be slightly different to plain {@link BooleanQuery} scorer mainly due to
 * differences in the {@link Similarity#coord(int,int) number of leave queries}
 * in the required boolean clause. In the most cases high-frequency terms are
 * unlikely to significantly contribute to the document score unless at least
 * one of the low-frequency terms are matched such that this query can improve
 * query execution times significantly if applicable.
 * <p>
 * {@link XCommonTermsQuery} has several advantages over stopword filtering at
 * index or query time since a term can be "classified" based on the actual
 * document frequency in the index and can prevent slow queries even across
 * domains without specialized stopword files.
 * </p>
 * <p>
 * <b>Note:</b> if the query only contains high-frequency terms the query is
 * rewritten into a plain conjunction query ie. all high-frequency terms need to
 * match in order to match a document.
 * </p>
 */
//LUCENE MONITOR - Copied from CommonTermsQuery changes are tracked with //CHANGE
public class XCommonTermsQuery extends Query {
  /*
   * TODO maybe it would make sense to abstract this even further and allow to
   * rewrite to dismax rather than boolean. Yet, this can already be subclassed
   * to do so.
   */
  protected final List<Term> terms = new ArrayList<Term>();
  protected final boolean disableCoord;
  protected final float maxTermFrequency;
  protected final Occur lowFreqOccur;
  protected final Occur highFreqOccur;
  protected float lowFreqBoost = 1.0f;
  protected float highFreqBoost = 1.0f;
  //CHANGE made minNr... a float for fractions
  protected float minNrShouldMatch = 0;
  
  /**
   * Creates a new {@link XCommonTermsQuery}
   * 
   * @param highFreqOccur
   *          {@link Occur} used for high frequency terms
   * @param lowFreqOccur
   *          {@link Occur} used for low frequency terms
   * @param maxTermFrequency
   *          a value in [0..1] (or absolute number >=1) representing the
   *          maximum threshold of a terms document frequency to be considered a
   *          low frequency term.
   * @throws IllegalArgumentException
   *           if {@link Occur#MUST_NOT} is pass as lowFreqOccur or
   *           highFreqOccur
   */
  public XCommonTermsQuery(Occur highFreqOccur, Occur lowFreqOccur,
      float maxTermFrequency) {
    this(highFreqOccur, lowFreqOccur, maxTermFrequency, false);
  }
  
  /**
   * Creates a new {@link XCommonTermsQuery}
   * 
   * @param highFreqOccur
   *          {@link Occur} used for high frequency terms
   * @param lowFreqOccur
   *          {@link Occur} used for low frequency terms
   * @param maxTermFrequency
   *          a value in [0..1] (or absolute number >=1) representing the
   *          maximum threshold of a terms document frequency to be considered a
   *          low frequency term.
   * @param disableCoord
   *          disables {@link Similarity#coord(int,int)} in scoring for the low
   *          / high frequency sub-queries
   * @throws IllegalArgumentException
   *           if {@link Occur#MUST_NOT} is pass as lowFreqOccur or
   *           highFreqOccur
   */
  public XCommonTermsQuery(Occur highFreqOccur, Occur lowFreqOccur,
      float maxTermFrequency, boolean disableCoord) {
    if (highFreqOccur == Occur.MUST_NOT) {
      throw new IllegalArgumentException(
          "highFreqOccur should be MUST or SHOULD but was MUST_NOT");
    }
    if (lowFreqOccur == Occur.MUST_NOT) {
      throw new IllegalArgumentException(
          "lowFreqOccur should be MUST or SHOULD but was MUST_NOT");
    }
    this.disableCoord = disableCoord;
    this.highFreqOccur = highFreqOccur;
    this.lowFreqOccur = lowFreqOccur;
    this.maxTermFrequency = maxTermFrequency;
  }
  
  /**
   * Adds a term to the {@link CommonTermsQuery}
   * 
   * @param term
   *          the term to add
   */
  public void add(Term term) {
    if (term == null) {
      throw new IllegalArgumentException("Term must not be null");
    }
    this.terms.add(term);
  }
  
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (this.terms.isEmpty()) {
      return new BooleanQuery();
    } else if (this.terms.size() == 1) {
      final TermQuery tq = new TermQuery(this.terms.get(0));
      tq.setBoost(getBoost());
      return tq;
    }
    final List<AtomicReaderContext> leaves = reader.leaves();
    final int maxDoc = reader.maxDoc();
    final TermContext[] contextArray = new TermContext[terms.size()];
    final Term[] queryTerms = this.terms.toArray(new Term[0]);
    collectTermContext(reader, leaves, contextArray, queryTerms);
    return buildQuery(maxDoc, contextArray, queryTerms);
  }
  
  //CHANGE added to get num optional
  protected int getMinimumNumberShouldMatch(int numOptional) {
      if (minNrShouldMatch >= 1.0f) {
          return (int) minNrShouldMatch;
      }
      return (int) (minNrShouldMatch * numOptional);
  }
  
  protected Query buildQuery(final int maxDoc,
      final TermContext[] contextArray, final Term[] queryTerms) {
    BooleanQuery lowFreq = new BooleanQuery(disableCoord);
    BooleanQuery highFreq = new BooleanQuery(disableCoord);
    highFreq.setBoost(highFreqBoost);
    lowFreq.setBoost(lowFreqBoost);
    
    BooleanQuery query = new BooleanQuery(true);
    
    for (int i = 0; i < queryTerms.length; i++) {
      TermContext termContext = contextArray[i];
      if (termContext == null) {
        lowFreq.add(new TermQuery(queryTerms[i]), lowFreqOccur);
      } else {
        if ((maxTermFrequency >= 1f && termContext.docFreq() > maxTermFrequency)
            || (termContext.docFreq() > (int) Math.ceil(maxTermFrequency
                * (float) maxDoc))) {
          highFreq
              .add(new TermQuery(queryTerms[i], termContext), highFreqOccur);
        } else {
          lowFreq.add(new TermQuery(queryTerms[i], termContext), lowFreqOccur);
        }
      }
      
    }
    if (lowFreqOccur == Occur.SHOULD) {
        lowFreq.setMinimumNumberShouldMatch(getMinimumNumberShouldMatch(lowFreq.clauses().size()));
    }
    if (lowFreq.clauses().isEmpty()) {
      /*
       * if lowFreq is empty we rewrite the high freq terms in a conjunction to
       * prevent slow queries.
       */
      if (highFreqOccur == Occur.MUST) {
        highFreq.setBoost(getBoost());
        return highFreq;
      } else {
        BooleanQuery highFreqConjunction = new BooleanQuery();
        for (BooleanClause booleanClause : highFreq) {
          highFreqConjunction.add(booleanClause.getQuery(), Occur.MUST);
        }
        highFreqConjunction.setBoost(getBoost());
        return highFreqConjunction;
        
      }
    } else if (highFreq.clauses().isEmpty()) {
      // only do low freq terms - we don't have high freq terms
      lowFreq.setBoost(getBoost());
      return lowFreq;
    } else {
      query.add(highFreq, Occur.SHOULD);
      query.add(lowFreq, Occur.MUST);
      query.setBoost(getBoost());
      return query;
    }
  }
  
  public void collectTermContext(IndexReader reader,
      List<AtomicReaderContext> leaves, TermContext[] contextArray,
      Term[] queryTerms) throws IOException {
    TermsEnum termsEnum = null;
    for (AtomicReaderContext context : leaves) {
      final Fields fields = context.reader().fields();
      if (fields == null) {
        // reader has no fields
        continue;
      }
      for (int i = 0; i < queryTerms.length; i++) {
        Term term = queryTerms[i];
        TermContext termContext = contextArray[i];
        final Terms terms = fields.terms(term.field());
        if (terms == null) {
          // field does not exist
          continue;
        }
        termsEnum = terms.iterator(termsEnum);
        assert termsEnum != null;
        
        if (termsEnum == TermsEnum.EMPTY) continue;
        if (termsEnum.seekExact(term.bytes(), false)) {
          if (termContext == null) {
            contextArray[i] = new TermContext(reader.getContext(),
                termsEnum.termState(), context.ord, termsEnum.docFreq(),
                termsEnum.totalTermFreq());
          } else {
            termContext.register(termsEnum.termState(), context.ord,
                termsEnum.docFreq(), termsEnum.totalTermFreq());
          }
          
        }
        
      }
    }
  }
  
  /**
   * Returns true iff {@link Similarity#coord(int,int)} is disabled in scoring
   * for the high and low frequency query instance. The top level query will
   * always disable coords.
   */
  public boolean isCoordDisabled() {
    return disableCoord;
  }
  
  /**
   * Specifies a minimum number of the optional BooleanClauses which must be
   * satisfied in order to produce a match on the low frequency terms query
   * part.
   * 
   * <p>
   * By default no optional clauses are necessary for a match (unless there are
   * no required clauses). If this method is used, then the specified number of
   * clauses is required.
   * </p>
   * 
   * @param min
   *          the number of optional clauses that must match
   */
  //CHANGE accepts now a float
  public void setMinimumNumberShouldMatch(float min) {
    this.minNrShouldMatch = min;
  }
  
  /**
   * Gets the minimum number of the optional BooleanClauses which must be
   * satisfied.
   */
  //CHANGE returns now a float
  public float getMinimumNumberShouldMatch() {
    return minNrShouldMatch;
  }
  
  @Override
  public void extractTerms(Set<Term> terms) {
    terms.addAll(this.terms);
  }
  
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    boolean needParens = (getBoost() != 1.0)
        || (getMinimumNumberShouldMatch() > 0);
    if (needParens) {
      buffer.append("(");
    }
    for (int i = 0; i < terms.size(); i++) {
      Term t = terms.get(i);
      buffer.append(new TermQuery(t).toString());
      
      if (i != terms.size() - 1) buffer.append(", ");
    }
    if (needParens) {
      buffer.append(")");
    }
    if (getMinimumNumberShouldMatch() > 0) {
      buffer.append('~');
      buffer.append(getMinimumNumberShouldMatch());
    }
    if (getBoost() != 1.0f) {
      buffer.append(ToStringUtils.boost(getBoost()));
    }
    return buffer.toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (disableCoord ? 1231 : 1237);
    result = prime * result + Float.floatToIntBits(highFreqBoost);
    result = prime * result
        + ((highFreqOccur == null) ? 0 : highFreqOccur.hashCode());
    result = prime * result + Float.floatToIntBits(lowFreqBoost);
    result = prime * result
        + ((lowFreqOccur == null) ? 0 : lowFreqOccur.hashCode());
    result = prime * result + Float.floatToIntBits(maxTermFrequency);
    result = prime * result + Float.floatToIntBits(minNrShouldMatch);
    result = prime * result + ((terms == null) ? 0 : terms.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    XCommonTermsQuery other = (XCommonTermsQuery) obj;
    if (disableCoord != other.disableCoord) return false;
    if (Float.floatToIntBits(highFreqBoost) != Float
        .floatToIntBits(other.highFreqBoost)) return false;
    if (highFreqOccur != other.highFreqOccur) return false;
    if (Float.floatToIntBits(lowFreqBoost) != Float
        .floatToIntBits(other.lowFreqBoost)) return false;
    if (lowFreqOccur != other.lowFreqOccur) return false;
    if (Float.floatToIntBits(maxTermFrequency) != Float
        .floatToIntBits(other.maxTermFrequency)) return false;
    if (minNrShouldMatch != other.minNrShouldMatch) return false;
    if (terms == null) {
      if (other.terms != null) return false;
    } else if (!terms.equals(other.terms)) return false;
    return true;
  }
  
  //CHANGE added
  public List<Term> terms() {
      return this.terms;
  }
  
}
