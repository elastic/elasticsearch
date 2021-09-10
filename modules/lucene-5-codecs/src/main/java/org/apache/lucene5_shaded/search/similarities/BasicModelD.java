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


import static org.apache.lucene5_shaded.search.similarities.SimilarityBase.log2;

/**
 * Implements the approximation of the binomial model with the divergence
 * for DFR. The formula used in Lucene differs slightly from the one in the
 * original paper: to avoid underflow for small values of {@code N} and
 * {@code F}, {@code N} is increased by {@code 1} and
 * {@code F} is always increased by {@code tfn+1}.
 * <p>
 * WARNING: for terms that do not meet the expected random distribution
 * (e.g. stopwords), this model may give poor performance, such as
 * abnormally high or NaN scores for low tf values.
 * @lucene.experimental
 */
public class BasicModelD extends BasicModel {
  
  /** Sole constructor: parameter-free */
  public BasicModelD() {}
  
  @Override
  public final float score(BasicStats stats, float tfn) {
    // we have to ensure phi is always < 1 for tiny TTF values, otherwise nphi can go negative,
    // resulting in NaN. cleanest way is to unconditionally always add tfn to totalTermFreq
    // to create a 'normalized' F.
    double F = stats.getTotalTermFreq() + 1 + tfn;
    double phi = (double)tfn / F;
    double nphi = 1 - phi;
    double p = 1.0 / (stats.getNumberOfDocuments() + 1);
    double D = phi * log2(phi / p) + nphi * log2(nphi / (1 - p));
    return (float)(D * F + 0.5 * log2(1 + 2 * Math.PI * tfn * nphi));
  }
  
  @Override
  public String toString() {
    return "D";
  }
}
