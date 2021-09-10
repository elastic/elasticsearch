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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene5_shaded.index.LeafReaderContext;

/**
 * A {@link Rescorer} that re-sorts according to a provided
 * Sort.
 */

public class SortRescorer extends Rescorer {

  private final Sort sort;

  /** Sole constructor. */
  public SortRescorer(Sort sort) {
    this.sort = sort;
  }

  @Override
  public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) throws IOException {

    // Copy ScoreDoc[] and sort by ascending docID:
    ScoreDoc[] hits = firstPassTopDocs.scoreDocs.clone();
    Arrays.sort(hits,
                new Comparator<ScoreDoc>() {
                  @Override
                  public int compare(ScoreDoc a, ScoreDoc b) {
                    return a.doc - b.doc;
                  }
                });

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    TopFieldCollector collector = TopFieldCollector.create(sort, topN, true, true, true);

    // Now merge sort docIDs from hits, with reader's leaves:
    int hitUpto = 0;
    int readerUpto = -1;
    int endDoc = 0;
    int docBase = 0;

    LeafCollector leafCollector = null;
    FakeScorer fakeScorer = new FakeScorer();

    while (hitUpto < hits.length) {
      ScoreDoc hit = hits[hitUpto];
      int docID = hit.doc;
      LeafReaderContext readerContext = null;
      while (docID >= endDoc) {
        readerUpto++;
        readerContext = leaves.get(readerUpto);
        endDoc = readerContext.docBase + readerContext.reader().maxDoc();
      }

      if (readerContext != null) {
        // We advanced to another segment:
        leafCollector = collector.getLeafCollector(readerContext);
        leafCollector.setScorer(fakeScorer);
        docBase = readerContext.docBase;
      }

      fakeScorer.score = hit.score;
      fakeScorer.doc = docID - docBase;

      leafCollector.collect(fakeScorer.doc);

      hitUpto++;
    }

    return collector.topDocs();
  }

  @Override
  public Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID) throws IOException {
    TopDocs oneHit = new TopDocs(1, new ScoreDoc[] {new ScoreDoc(docID, firstPassExplanation.getValue())});
    TopDocs hits = rescore(searcher, oneHit, 1);
    assert hits.totalHits == 1;

    List<Explanation> subs = new ArrayList<>();

    // Add first pass:
    Explanation first = Explanation.match(firstPassExplanation.getValue(), "first pass score", firstPassExplanation);
    subs.add(first);

    FieldDoc fieldDoc = (FieldDoc) hits.scoreDocs[0];

    // Add sort values:
    SortField[] sortFields = sort.getSort();
    for(int i=0;i<sortFields.length;i++) {
      subs.add(Explanation.match(0.0f, "sort field " + sortFields[i].toString() + " value=" + fieldDoc.fields[i]));
    }

    // TODO: if we could ask the Sort to explain itself then
    // we wouldn't need the separate ExpressionRescorer...
    return Explanation.match(0.0f, "sort field values for sort=" + sort.toString(), subs);
  }
}
