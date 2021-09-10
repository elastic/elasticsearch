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
 * This class acts as the base class for the implementations of the <em>first
 * normalization of the informative content</em> in the DFR framework. This
 * component is also called the <em>after effect</em> and is defined by the
 * formula <em>Inf<sub>2</sub> = 1 - Prob<sub>2</sub></em>, where
 * <em>Prob<sub>2</sub></em> measures the <em>information gain</em>.
 * 
 * @see DFRSimilarity
 * @lucene.experimental
 */
public abstract class AfterEffect {
  
  /**
   * Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.)
   */
  public AfterEffect() {}

  /** Returns the aftereffect score. */
  public abstract float score(BasicStats stats, float tfn);
  
  /** Returns an explanation for the score. */
  public abstract Explanation explain(BasicStats stats, float tfn);

  /** Implementation used when there is no aftereffect. */
  public static final class NoAfterEffect extends AfterEffect {
    
    /** Sole constructor: parameter-free */
    public NoAfterEffect() {}
    
    @Override
    public final float score(BasicStats stats, float tfn) {
      return 1f;
    }

    @Override
    public final Explanation explain(BasicStats stats, float tfn) {
      return Explanation.match(1, "no aftereffect");
    }
    
    @Override
    public String toString() {
      return "";
    }
  }
  
  /**
   * Subclasses must override this method to return the code of the
   * after effect formula. Refer to the original paper for the list. 
   */
  @Override
  public abstract String toString();
}
