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
package org.apache.lucene5_shaded.search.similarities;


/**
 * Implements the <em>Divergence from Independence (DFI)</em> model based on Chi-square statistics
 * (i.e., standardized Chi-squared distance from independence in term frequency tf).
 * <p>
 * DFI is both parameter-free and non-parametric:
 * <ul>
 * <li>parameter-free: it does not require any parameter tuning or training.</li>
 * <li>non-parametric: it does not make any assumptions about word frequency distributions on document collections.</li>
 * </ul>
 * <p>
 * It is highly recommended <b>not</b> to remove stopwords (very common terms: the, of, and, to, a, in, for, is, on, that, etc) with this similarity.
 * <p>
 * For more information see: <a href="http://dx.doi.org/10.1007/s10791-013-9225-4">A nonparametric term weighting method for information retrieval based on measuring the divergence from independence</a>
 *
 * @lucene.experimental
 * @see IndependenceStandardized
 * @see IndependenceSaturated
 * @see IndependenceChiSquared
 */


public class DFISimilarity extends SimilarityBase {
  private final Independence independence;
  
  /**
   * Create DFI with the specified divergence from independence measure
   * @param independenceMeasure measure of divergence from independence
   */
  public DFISimilarity(Independence independenceMeasure) {
    this.independence = independenceMeasure;
  }

  @Override
  protected float score(BasicStats stats, float freq, float docLen) {

    final float expected = (stats.getTotalTermFreq() + 1) * docLen / (stats.getNumberOfFieldTokens() + 1);

    // if the observed frequency is less than or equal to the expected value, then return zero.
    if (freq <= expected) return 0;

    final float measure = independence.score(freq, expected);

    return stats.getBoost() * (float) log2(measure + 1);
  }

  /**
   * Returns the measure of independence
   */
  public Independence getIndependence() {
    return independence;
  }

  @Override
  public String toString() {
    return "DFI(" + independence + ")";
  }
}

