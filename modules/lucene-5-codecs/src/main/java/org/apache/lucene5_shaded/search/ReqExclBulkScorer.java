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

import org.apache.lucene5_shaded.util.Bits;

final class ReqExclBulkScorer extends BulkScorer {

  private final BulkScorer req;
  private final DocIdSetIterator excl;

  ReqExclBulkScorer(BulkScorer req, DocIdSetIterator excl) {
    this.req = req;
    this.excl = excl;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    int upTo = min;
    int exclDoc = excl.docID();

    while (upTo < max) {
      if (exclDoc < upTo) {
        exclDoc = excl.advance(upTo);
      }
      if (exclDoc == upTo) {
        // upTo is excluded so we can consider that we scored up to upTo+1
        upTo += 1;
        exclDoc = excl.nextDoc();
      } else {
        upTo = req.score(collector, acceptDocs, upTo, Math.min(exclDoc, max));
      }
    }

    if (upTo == max) {
      upTo = req.score(collector, acceptDocs, upTo, upTo);
    }

    return upTo;
  }

  @Override
  public long cost() {
    return req.cost();
  }

}
