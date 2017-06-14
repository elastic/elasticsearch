/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.search;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.similarities.BasicStats;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class XToParentBlockJoinQueryTests extends ESTestCase {

    public void testScoreMode() throws IOException {
        Similarity sim = new SimilarityBase() {

          @Override
          public String toString() {
            return "TestSim";
          }

          @Override
          protected float score(BasicStats stats, float freq, float docLen) {
            return freq;
          }
        };
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setSimilarity(sim));
        w.addDocuments(Arrays.asList(
            Collections.singleton(newTextField("foo", "bar bar", Store.NO)),
            Collections.singleton(newTextField("foo", "bar", Store.NO)),
            Collections.emptyList(),
            Collections.singleton(newStringField("type", new BytesRef("parent"), Store.NO))));
        DirectoryReader reader = w.getReader();
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        searcher.setSimilarity(sim);
        BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
        for (ScoreMode scoreMode : ScoreMode.values()) {
          Query query = new XToParentBlockJoinQuery(new TermQuery(new Term("foo", "bar")), parents, scoreMode);
          TopDocs topDocs = searcher.search(query, 10);
          assertEquals(1, topDocs.totalHits);
          assertEquals(3, topDocs.scoreDocs[0].doc);
          float expectedScore;
          switch (scoreMode) {
            case Avg:
              expectedScore = 1.5f;
              break;
            case Max:
              expectedScore = 2f;
              break;
            case Min:
              expectedScore = 1f;
              break;
            case None:
              expectedScore = 0f;
              break;
            case Total:
              expectedScore = 3f;
              break;
            default:
              throw new AssertionError();
          }
          assertEquals(expectedScore, topDocs.scoreDocs[0].score, 0f);
        }
        reader.close();
        dir.close();
      }

}
