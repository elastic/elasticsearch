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
package org.apache.lucene5_shaded.search;


import java.io.IOException;

/**
 * Re-scores the topN results ({@link TopDocs}) from an original
 * query.  See {@link QueryRescorer} for an actual
 * implementation.  Typically, you run a low-cost
 * first-pass query across the entire index, collecting the
 * top few hundred hits perhaps, and then use this class to
 * mix in a more costly second pass scoring.
 *
 * <p>See {@link
 * QueryRescorer#rescore(IndexSearcher,TopDocs,Query,double,int)}
 * for a simple static method to call to rescore using a 2nd
 * pass {@link Query}.
 *
 * @lucene.experimental
 */

public abstract class Rescorer {

  /** 
   * Rescore an initial first-pass {@link TopDocs}.
   *
   * @param searcher {@link IndexSearcher} used to produce the
   *   first pass topDocs
   * @param firstPassTopDocs Hits from the first pass
   *   search.  It's very important that these hits were
   *   produced by the provided searcher; otherwise the doc
   *   IDs will not match!
   * @param topN How many re-scored hits to return
   */ 
  public abstract TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) throws IOException;

  /**
   * Explains how the score for the specified document was
   * computed.
   */
  public abstract Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID) throws IOException;
}
