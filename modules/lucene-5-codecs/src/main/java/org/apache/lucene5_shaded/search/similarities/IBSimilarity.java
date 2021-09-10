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


import java.util.List;

import org.apache.lucene5_shaded.search.Explanation;
import org.apache.lucene5_shaded.search.similarities.Normalization.NoNormalization;

/**
 * Provides a framework for the family of information-based models, as described
 * in St&eacute;phane Clinchant and Eric Gaussier. 2010. Information-based
 * models for ad hoc IR. In Proceeding of the 33rd international ACM SIGIR
 * conference on Research and development in information retrieval (SIGIR '10).
 * ACM, New York, NY, USA, 234-241.
 * <p>The retrieval function is of the form <em>RSV(q, d) = &sum;
 * -x<sup>q</sup><sub>w</sub> log Prob(X<sub>w</sub> &ge;
 * t<sup>d</sup><sub>w</sub> | &lambda;<sub>w</sub>)</em>, where
 * <ul>
 *   <li><em>x<sup>q</sup><sub>w</sub></em> is the query boost;</li>
 *   <li><em>X<sub>w</sub></em> is a random variable that counts the occurrences
 *   of word <em>w</em>;</li>
 *   <li><em>t<sup>d</sup><sub>w</sub></em> is the normalized term frequency;</li>
 *   <li><em>&lambda;<sub>w</sub></em> is a parameter.</li>
 * </ul>
 * <p>The framework described in the paper has many similarities to the DFR
 * framework (see {@link DFRSimilarity}). It is possible that the two
 * Similarities will be merged at one point.</p>
 * <p>To construct an IBSimilarity, you must specify the implementations for 
 * all three components of the Information-Based model.
 * <ol>
 *     <li>{@link Distribution}: Probabilistic distribution used to
 *         model term occurrence
 *         <ul>
 *             <li>{@link DistributionLL}: Log-logistic</li>
 *             <li>{@link DistributionLL}: Smoothed power-law</li>
 *         </ul>
 *     </li>
 *     <li>{@link Lambda}: &lambda;<sub>w</sub> parameter of the
 *         probability distribution
 *         <ul>
 *             <li>{@link LambdaDF}: <code>N<sub>w</sub>/N</code> or average
 *                 number of documents where w occurs</li>
 *             <li>{@link LambdaTTF}: <code>F<sub>w</sub>/N</code> or
 *                 average number of occurrences of w in the collection</li>
 *         </ul>
 *     </li>
 *     <li>{@link Normalization}: Term frequency normalization 
 *         <blockquote>Any supported DFR normalization (listed in
 *                      {@link DFRSimilarity})</blockquote>
 *     </li>
 * </ol>
 * @see DFRSimilarity
 * @lucene.experimental 
 */
public class IBSimilarity extends SimilarityBase {
  /** The probabilistic distribution used to model term occurrence. */
  protected final Distribution distribution;
  /** The <em>lambda (&lambda;<sub>w</sub>)</em> parameter. */
  protected final Lambda lambda;
  /** The term frequency normalization. */
  protected final Normalization normalization;
  
  /**
   * Creates IBSimilarity from the three components.
   * <p>
   * Note that <code>null</code> values are not allowed:
   * if you want no normalization, instead pass 
   * {@link NoNormalization}.
   * @param distribution probabilistic distribution modeling term occurrence
   * @param lambda distribution's &lambda;<sub>w</sub> parameter
   * @param normalization term frequency normalization
   */
  public IBSimilarity(Distribution distribution,
                      Lambda lambda,
                      Normalization normalization) {
    this.distribution = distribution;
    this.lambda = lambda;
    this.normalization = normalization;
  }
  
  @Override
  protected float score(BasicStats stats, float freq, float docLen) {
    return stats.getBoost() *
        distribution.score(
            stats,
            normalization.tfn(stats, freq, docLen),
            lambda.lambda(stats));
  }

  @Override
  protected void explain(
      List<Explanation> subs, BasicStats stats, int doc, float freq, float docLen) {
    if (stats.getBoost() != 1.0f) {
      subs.add(Explanation.match(stats.getBoost(), "boost"));
    }
    Explanation normExpl = normalization.explain(stats, freq, docLen);
    Explanation lambdaExpl = lambda.explain(stats);
    subs.add(normExpl);
    subs.add(lambdaExpl);
    subs.add(distribution.explain(stats, normExpl.getValue(), lambdaExpl.getValue()));
  }
  
  /**
   * The name of IB methods follow the pattern
   * {@code IB <distribution> <lambda><normalization>}. The name of the
   * distribution is the same as in the original paper; for the names of lambda
   * parameters, refer to the javadoc of the {@link Lambda} classes.
   */
  @Override
  public String toString() {
    return "IB " + distribution.toString() + "-" + lambda.toString()
                 + normalization.toString();
  }
  
  /**
   * Returns the distribution
   */
  public Distribution getDistribution() {
    return distribution;
  }
  
  /**
   * Returns the distribution's lambda parameter
   */
  public Lambda getLambda() {
    return lambda;
  }

  /**
   * Returns the term frequency normalization
   */
  public Normalization getNormalization() {
    return normalization;
  }
}
