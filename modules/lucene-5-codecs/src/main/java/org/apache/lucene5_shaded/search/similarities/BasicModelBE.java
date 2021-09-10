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
 * Limiting form of the Bose-Einstein model. The formula used in Lucene differs
 * slightly from the one in the original paper: {@code F} is increased by {@code tfn+1}
 * and {@code N} is increased by {@code F} 
 * @lucene.experimental
 * NOTE: in some corner cases this model may give poor performance or infinite scores with 
 * Normalizations that return large or small values for {@code tfn} such as NormalizationH3. 
 * Consider using the geometric approximation ({@link BasicModelG}) instead, which provides 
 * the same relevance but with less practical problems. 
 */
public class BasicModelBE extends BasicModel {
  
  /** Sole constructor: parameter-free */
  public BasicModelBE() {}

  @Override
  public final float score(BasicStats stats, float tfn) {
    double F = stats.getTotalTermFreq() + 1 + tfn;
    // approximation only holds true when F << N, so we use N += F
    double N = F + stats.getNumberOfDocuments();
    return (float)(-log2((N - 1) * Math.E)
        + f(N + F - 1, N + F - tfn - 2) - f(F, F - tfn));
  }
  
  /** The <em>f</em> helper function defined for <em>B<sub>E</sub></em>. */
  private final double f(double n, double m) {
    return (m + 0.5) * log2(n / m) + (n - m) * log2(n);
  }
  
  @Override
  public String toString() {
    return "Be";
  }
}
