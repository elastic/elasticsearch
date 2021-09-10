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

import org.apache.lucene5_shaded.index.SingleTermsEnum;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.AttributeSource;
import org.apache.lucene5_shaded.util.ToStringUtils;
import org.apache.lucene5_shaded.util.automaton.LevenshteinAutomata;

/** Implements the fuzzy search query. The similarity measurement
 * is based on the Damerau-Levenshtein (optimal string alignment) algorithm,
 * though you can explicitly choose classic Levenshtein by passing <code>false</code>
 * to the <code>transpositions</code> parameter.
 * 
 * <p>This query uses {@link TopTermsScoringBooleanQueryRewrite}
 * as default. So terms will be collected and scored according to their
 * edit distance. Only the top terms are used for building the {@link BooleanQuery}.
 * It is not recommended to change the rewrite mode for fuzzy queries.
 * 
 * <p>At most, this query will match terms up to 
 * {@value LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE} edits.
 * Higher distances (especially with transpositions enabled), are generally not useful and 
 * will match a significant amount of the term dictionary. If you really want this, consider
 * using an n-gram indexing technique (such as the SpellChecker in the 
 * <a href="{@docRoot}/../suggest/overview-summary.html">suggest module</a>) instead.
 *
 * <p>NOTE: terms of length 1 or 2 will sometimes not match because of how the scaled
 * distance between two terms is computed.  For a term to match, the edit distance between
 * the terms must be less than the minimum length term (either the input term, or
 * the candidate term).  For example, FuzzyQuery on term "abcd" with maxEdits=2 will
 * not match an indexed term "ab", and FuzzyQuery on term "a" with maxEdits=2 will not
 * match an indexed term "abc".
 */
public class FuzzyQuery extends MultiTermQuery {
  
  public final static int defaultMaxEdits = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
  public final static int defaultPrefixLength = 0;
  public final static int defaultMaxExpansions = 50;
  public final static boolean defaultTranspositions = true;
  
  private final int maxEdits;
  private final int maxExpansions;
  private final boolean transpositions;
  private final int prefixLength;
  private final Term term;
  
  /**
   * Create a new FuzzyQuery that will match terms with an edit distance 
   * of at most <code>maxEdits</code> to <code>term</code>.
   * If a <code>prefixLength</code> &gt; 0 is specified, a common prefix
   * of that length is also required.
   * 
   * @param term the term to search for
   * @param maxEdits must be {@code >= 0} and {@code <=} {@link LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}.
   * @param prefixLength length of common (non-fuzzy) prefix
   * @param maxExpansions the maximum number of terms to match. If this number is
   *  greater than {@link BooleanQuery#getMaxClauseCount} when the query is rewritten, 
   *  then the maxClauseCount will be used instead.
   * @param transpositions true if transpositions should be treated as a primitive
   *        edit operation. If this is false, comparisons will implement the classic
   *        Levenshtein algorithm.
   */
  public FuzzyQuery(Term term, int maxEdits, int prefixLength, int maxExpansions, boolean transpositions) {
    super(term.field());
    
    if (maxEdits < 0 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
      throw new IllegalArgumentException("maxEdits must be between 0 and " + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE);
    }
    if (prefixLength < 0) {
      throw new IllegalArgumentException("prefixLength cannot be negative.");
    }
    if (maxExpansions <= 0) {
      throw new IllegalArgumentException("maxExpansions must be positive.");
    }
    
    this.term = term;
    this.maxEdits = maxEdits;
    this.prefixLength = prefixLength;
    this.transpositions = transpositions;
    this.maxExpansions = maxExpansions;
    setRewriteMethod(new TopTermsBlendedFreqScoringRewrite(maxExpansions));
  }
  
  /**
   * Calls {@link #FuzzyQuery(Term, int, int, int, boolean) 
   * FuzzyQuery(term, maxEdits, prefixLength, defaultMaxExpansions, defaultTranspositions)}.
   */
  public FuzzyQuery(Term term, int maxEdits, int prefixLength) {
    this(term, maxEdits, prefixLength, defaultMaxExpansions, defaultTranspositions);
  }
  
  /**
   * Calls {@link #FuzzyQuery(Term, int, int) FuzzyQuery(term, maxEdits, defaultPrefixLength)}.
   */
  public FuzzyQuery(Term term, int maxEdits) {
    this(term, maxEdits, defaultPrefixLength);
  }

  /**
   * Calls {@link #FuzzyQuery(Term, int) FuzzyQuery(term, defaultMaxEdits)}.
   */
  public FuzzyQuery(Term term) {
    this(term, defaultMaxEdits);
  }
  
  /**
   * @return the maximum number of edit distances allowed for this query to match.
   */
  public int getMaxEdits() {
    return maxEdits;
  }
    
  /**
   * Returns the non-fuzzy prefix length. This is the number of characters at the start
   * of a term that must be identical (not fuzzy) to the query term if the query
   * is to match that term. 
   */
  public int getPrefixLength() {
    return prefixLength;
  }
  
  /**
   * Returns true if transpositions should be treated as a primitive edit operation. 
   * If this is false, comparisons will implement the classic Levenshtein algorithm.
   */
  public boolean getTranspositions() {
    return transpositions;
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    if (maxEdits == 0 || prefixLength >= term.text().length()) {  // can only match if it's exact
      return new SingleTermsEnum(terms.iterator(), term.bytes());
    }
    return new FuzzyTermsEnum(terms, atts, getTerm(), maxEdits, prefixLength, transpositions);
  }
  
  /**
   * Returns the pattern term.
   */
  public Term getTerm() {
    return term;
  }
    
  @Override
  public String toString(String field) {
    final StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
        buffer.append(term.field());
        buffer.append(":");
    }
    buffer.append(term.text());
    buffer.append('~');
    buffer.append(Integer.toString(maxEdits));
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + maxEdits;
    result = prime * result + prefixLength;
    result = prime * result + maxExpansions;
    result = prime * result + (transpositions ? 0 : 1);
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    FuzzyQuery other = (FuzzyQuery) obj;
    if (maxEdits != other.maxEdits)
      return false;
    if (prefixLength != other.prefixLength)
      return false;
    if (maxExpansions != other.maxExpansions)
      return false;
    if (transpositions != other.transpositions)
      return false;
    if (term == null) {
      if (other.term != null)
        return false;
    } else if (!term.equals(other.term))
      return false;
    return true;
  }
  
  /**
   * @deprecated pass integer edit distances instead.
   */
  @Deprecated
  public final static float defaultMinSimilarity = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;

  /**
   * Helper function to convert from deprecated "minimumSimilarity" fractions
   * to raw edit distances.
   * 
   * @param minimumSimilarity scaled similarity
   * @param termLen length (in unicode codepoints) of the term.
   * @return equivalent number of maxEdits
   * @deprecated pass integer edit distances instead.
   */
  @Deprecated
  public static int floatToEdits(float minimumSimilarity, int termLen) {
    if (minimumSimilarity >= 1f) {
      return (int) Math.min(minimumSimilarity, LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE);
    } else if (minimumSimilarity == 0.0f) {
      return 0; // 0 means exact, not infinite # of edits!
    } else {
      return Math.min((int) ((1D-minimumSimilarity) * termLen), 
        LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE);
    }
  }
}
