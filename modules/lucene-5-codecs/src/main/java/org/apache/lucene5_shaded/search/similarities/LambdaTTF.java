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
 * Computes lambda as {@code totalTermFreq+1 / numberOfDocuments+1}.
 * @lucene.experimental
 */
public class LambdaTTF extends Lambda {  
  
  /** Sole constructor: parameter-free */
  public LambdaTTF() {}

  @Override
  public final float lambda(BasicStats stats) {
    return (stats.getTotalTermFreq()+1F) / (stats.getNumberOfDocuments()+1F);
  }

  @Override
  public final Explanation explain(BasicStats stats) {
    return Explanation.match(
        lambda(stats),
        getClass().getSimpleName() + ", computed from: ",
        Explanation.match(stats.getTotalTermFreq(), "totalTermFreq"),
        Explanation.match(stats.getNumberOfDocuments(), "numberOfDocuments"));
  }
  
  @Override
  public String toString() {
    return "L";
  }
}
