package org.apache.lucene.search.suggest.xdocument;

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

import org.apache.lucene.util.PriorityQueue;

/**
 * Bounded priority queue for {@link TopSuggestDocs.SuggestScoreDoc}s.
 * Priority is based on {@link TopSuggestDocs.SuggestScoreDoc#score} and tie
 * is broken by {@link TopSuggestDocs.SuggestScoreDoc#doc}
 */
final class SuggestScoreDocPriorityQueue extends PriorityQueue<TopSuggestDocs.SuggestScoreDoc> {
  /**
   * Creates a new priority queue of the specified size.
   */
  public SuggestScoreDocPriorityQueue(int size) {
    super(size);
  }

  @Override
  protected boolean lessThan(TopSuggestDocs.SuggestScoreDoc a, TopSuggestDocs.SuggestScoreDoc b) {
    if (a.score == b.score) {
      // prefer smaller doc id, in case of a tie
      return a.doc > b.doc;
    }
    return a.score < b.score;
  }

  /**
   * Returns the top N results in descending order.
   */
  public TopSuggestDocs.SuggestScoreDoc[] getResults() {
    int size = size();
    TopSuggestDocs.SuggestScoreDoc[] res = new TopSuggestDocs.SuggestScoreDoc[size];
    for (int i = size - 1; i >= 0; i--) {
      res[i] = pop();
    }
    return res;
  }
}
