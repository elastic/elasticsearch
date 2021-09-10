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
import org.apache.lucene5_shaded.search.similarities.AfterEffect.NoAfterEffect;
import org.apache.lucene5_shaded.search.similarities.Normalization.NoNormalization;

/**
 * Implements the <em>divergence from randomness (DFR)</em> framework
 * introduced in Gianni Amati and Cornelis Joost Van Rijsbergen. 2002.
 * Probabilistic models of information retrieval based on measuring the
 * divergence from randomness. ACM Trans. Inf. Syst. 20, 4 (October 2002),
 * 357-389.
 * <p>The DFR scoring formula is composed of three separate components: the
 * <em>basic model</em>, the <em>aftereffect</em> and an additional
 * <em>normalization</em> component, represented by the classes
 * {@code BasicModel}, {@code AfterEffect} and {@code Normalization},
 * respectively. The names of these classes were chosen to match the names of
 * their counterparts in the Terrier IR engine.</p>
 * <p>To construct a DFRSimilarity, you must specify the implementations for 
 * all three components of DFR:
 * <ol>
 *    <li>{@link BasicModel}: Basic model of information content:
 *        <ul>
 *           <li>{@link BasicModelBE}: Limiting form of Bose-Einstein
 *           <li>{@link BasicModelG}: Geometric approximation of Bose-Einstein
 *           <li>{@link BasicModelP}: Poisson approximation of the Binomial
 *           <li>{@link BasicModelD}: Divergence approximation of the Binomial 
 *           <li>{@link BasicModelIn}: Inverse document frequency
 *           <li>{@link BasicModelIne}: Inverse expected document
 *               frequency [mixture of Poisson and IDF]
 *           <li>{@link BasicModelIF}: Inverse term frequency
 *               [approximation of I(ne)]
 *        </ul>
 *    <li>{@link AfterEffect}: First normalization of information
 *        gain:
 *        <ul>
 *           <li>{@link AfterEffectL}: Laplace's law of succession
 *           <li>{@link AfterEffectB}: Ratio of two Bernoulli processes
 *           <li>{@link NoAfterEffect}: no first normalization
 *        </ul>
 *    <li>{@link Normalization}: Second (length) normalization:
 *        <ul>
 *           <li>{@link NormalizationH1}: Uniform distribution of term
 *               frequency
 *           <li>{@link NormalizationH2}: term frequency density inversely
 *               related to length
 *           <li>{@link NormalizationH3}: term frequency normalization
 *               provided by Dirichlet prior
 *           <li>{@link NormalizationZ}: term frequency normalization provided
 *                by a Zipfian relation
 *           <li>{@link NoNormalization}: no second normalization
 *        </ul>
 * </ol>
 * <p>Note that <em>qtf</em>, the multiplicity of term-occurrence in the query,
 * is not handled by this implementation.</p>
 * @see BasicModel
 * @see AfterEffect
 * @see Normalization
 * @lucene.experimental
 */
public class DFRSimilarity extends SimilarityBase {
  /** The basic model for information content. */
  protected final BasicModel basicModel;
  /** The first normalization of the information content. */
  protected final AfterEffect afterEffect;
  /** The term frequency normalization. */
  protected final Normalization normalization;
  
  /**
   * Creates DFRSimilarity from the three components.
   * <p>
   * Note that <code>null</code> values are not allowed:
   * if you want no normalization or after-effect, instead pass 
   * {@link NoNormalization} or {@link NoAfterEffect} respectively.
   * @param basicModel Basic model of information content
   * @param afterEffect First normalization of information gain
   * @param normalization Second (length) normalization
   */
  public DFRSimilarity(BasicModel basicModel,
                       AfterEffect afterEffect,
                       Normalization normalization) {
    if (basicModel == null || afterEffect == null || normalization == null) {
      throw new NullPointerException("null parameters not allowed.");
    }
    this.basicModel = basicModel;
    this.afterEffect = afterEffect;
    this.normalization = normalization;
  }

  @Override
  protected float score(BasicStats stats, float freq, float docLen) {
    float tfn = normalization.tfn(stats, freq, docLen);
    return stats.getBoost() *
        basicModel.score(stats, tfn) * afterEffect.score(stats, tfn);
  }
  
  @Override
  protected void explain(List<Explanation> subs,
      BasicStats stats, int doc, float freq, float docLen) {
    if (stats.getBoost() != 1.0f) {
      subs.add(Explanation.match(stats.getBoost(), "boost"));
    }
    
    Explanation normExpl = normalization.explain(stats, freq, docLen);
    float tfn = normExpl.getValue();
    subs.add(normExpl);
    subs.add(basicModel.explain(stats, tfn));
    subs.add(afterEffect.explain(stats, tfn));
  }

  @Override
  public String toString() {
    return "DFR " + basicModel.toString() + afterEffect.toString()
                  + normalization.toString();
  }
  
  /**
   * Returns the basic model of information content
   */
  public BasicModel getBasicModel() {
    return basicModel;
  }
  
  /**
   * Returns the first normalization
   */
  public AfterEffect getAfterEffect() {
    return afterEffect;
  }
  
  /**
   * Returns the second normalization
   */
  public Normalization getNormalization() {
    return normalization;
  }
}
