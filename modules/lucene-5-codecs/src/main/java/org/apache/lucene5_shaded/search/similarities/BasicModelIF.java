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
 * An approximation of the <em>I(n<sub>e</sub>)</em> model.
 * @lucene.experimental
 */ 
public class BasicModelIF extends BasicModel {
  
  /** Sole constructor: parameter-free */
  public BasicModelIF() {}

  @Override
  public final float score(BasicStats stats, float tfn) {
    long N = stats.getNumberOfDocuments();
    long F = stats.getTotalTermFreq();
    return tfn * (float)(log2(1 + (N + 1) / (F + 0.5)));
  }

  @Override
  public String toString() {
    return "I(F)";
  }
}
