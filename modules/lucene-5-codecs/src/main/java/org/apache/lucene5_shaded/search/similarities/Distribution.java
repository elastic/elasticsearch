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


import org.apache.lucene5_shaded.search.Explanation;

/**
 * The probabilistic distribution used to model term occurrence
 * in information-based models.
 * @see IBSimilarity
 * @lucene.experimental
 */
public abstract class Distribution {
  
  /**
   * Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.)
   */
  public Distribution() {}

  /** Computes the score. */
  public abstract float score(BasicStats stats, float tfn, float lambda);
  
  /** Explains the score. Returns the name of the model only, since
   * both {@code tfn} and {@code lambda} are explained elsewhere. */
  public Explanation explain(BasicStats stats, float tfn, float lambda) {
    return Explanation.match(
        score(stats, tfn, lambda), getClass().getSimpleName());
  }
  
  /**
   * Subclasses must override this method to return the name of the
   * distribution. 
   */
  @Override
  public abstract String toString();
}
