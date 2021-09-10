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
 * Model of the information gain based on the ratio of two Bernoulli processes.
 * @lucene.experimental
 */
public class AfterEffectB extends AfterEffect {

  /** Sole constructor: parameter-free */
  public AfterEffectB() {}

  @Override
  public final float score(BasicStats stats, float tfn) {
    long F = stats.getTotalTermFreq()+1;
    long n = stats.getDocFreq()+1;
    return (F + 1) / (n * (tfn + 1));
  }
  
  @Override
  public final Explanation explain(BasicStats stats, float tfn) {
    return Explanation.match(
        score(stats, tfn),
        getClass().getSimpleName() + ", computed from: ",
        Explanation.match(tfn, "tfn"),
        Explanation.match(stats.getTotalTermFreq(), "totalTermFreq"),
        Explanation.match(stats.getDocFreq(), "docFreq"));
  }

  @Override
  public String toString() {
    return "B";
  }
}
