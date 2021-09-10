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
 * Computes the measure of divergence from independence for DFI
 * scoring functions.
 * <p>
 * See http://trec.nist.gov/pubs/trec21/papers/irra.web.nb.pdf for more information
 * on different methods.
 * @lucene.experimental
 */
public abstract class Independence {

  /**
   * Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.)
   */
  public Independence() {}
  
  /**
   * Computes distance from independence
   * @param freq actual term frequency
   * @param expected expected term frequency
   */
  public abstract float score(float freq, float expected);
  
  // subclasses must provide a name
  @Override
  public abstract String toString();
}
