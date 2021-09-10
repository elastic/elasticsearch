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
 * Dirichlet Priors normalization
 * @lucene.experimental
 */
public class NormalizationH3 extends Normalization {
  private final float mu;
  
  /**
   * Calls {@link #NormalizationH3(float) NormalizationH3(800)}
   */
  public NormalizationH3() {
    this(800F);
  }
  
  /**
   * Creates NormalizationH3 with the supplied parameter <code>&mu;</code>.
   * @param mu smoothing parameter <code>&mu;</code>
   */
  public NormalizationH3(float mu) {
    this.mu = mu;
  }

  @Override
  public float tfn(BasicStats stats, float tf, float len) {
    return (tf + mu * ((stats.getTotalTermFreq()+1F) / (stats.getNumberOfFieldTokens()+1F))) / (len + mu) * mu;
  }

  @Override
  public String toString() {
    return "3(" + mu + ")";
  }
  
  /**
   * Returns the parameter <code>&mu;</code>
   * @see #NormalizationH3(float)
   */
  public float getMu() {
    return mu;
  }
}
