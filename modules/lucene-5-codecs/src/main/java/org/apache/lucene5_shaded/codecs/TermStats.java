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
package org.apache.lucene5_shaded.codecs;


import org.apache.lucene5_shaded.index.TermsEnum; // javadocs

/**
 * Holder for per-term statistics.
 * 
 * @see TermsEnum#docFreq
 * @see TermsEnum#totalTermFreq
 */
public class TermStats {
  /** How many documents have at least one occurrence of
   *  this term. */
  public final int docFreq;
  
  /** Total number of times this term occurs across all
   *  documents in the field. */
  public final long totalTermFreq;

  /** Sole constructor. */
  public TermStats(int docFreq, long totalTermFreq) {
    this.docFreq = docFreq;
    this.totalTermFreq = totalTermFreq;
  }
}
