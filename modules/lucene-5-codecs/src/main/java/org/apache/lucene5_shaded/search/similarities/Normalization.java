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
 * This class acts as the base class for the implementations of the term
 * frequency normalization methods in the DFR framework.
 * 
 * @see DFRSimilarity
 * @lucene.experimental
 */
public abstract class Normalization {
  
  /**
   * Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.)
   */
  public Normalization() {}

  /** Returns the normalized term frequency.
   * @param len the field length. */
  public abstract float tfn(BasicStats stats, float tf, float len);
  
  /** Returns an explanation for the normalized term frequency.
   * <p>The default normalization methods use the field length of the document
   * and the average field length to compute the normalized term frequency.
   * This method provides a generic explanation for such methods.
   * Subclasses that use other statistics must override this method.</p>
   */
  public Explanation explain(BasicStats stats, float tf, float len) {
    return Explanation.match(
        tfn(stats, tf, len),
        getClass().getSimpleName() + ", computed from: ",
        Explanation.match(tf, "tf"),
        Explanation.match(stats.getAvgFieldLength(), "avgFieldLength"),
        Explanation.match(len, "len"));
  }

  /** Implementation used when there is no normalization. */
  public static final class NoNormalization extends Normalization {
    
    /** Sole constructor: parameter-free */
    public NoNormalization() {}
    
    @Override
    public final float tfn(BasicStats stats, float tf, float len) {
      return tf;
    }

    @Override
    public final Explanation explain(BasicStats stats, float tf, float len) {
      return Explanation.match(1, "no normalization");
    }
    
    @Override
    public String toString() {
      return "";
    }
  }
  
  /**
   * Subclasses must override this method to return the code of the
   * normalization formula. Refer to the original paper for the list. 
   */
  @Override
  public abstract String toString();
}
