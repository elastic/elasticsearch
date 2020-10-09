/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.queries;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

import java.time.Instant;

public class TwoPhaseLongDistanceFeatureQueryTests extends LuceneTestCase {

    public void testIndexAndQuerySmall() throws Exception {
        dotestIndexAndQuery(random().nextInt(10));
    }

    public void testIndexAndQueryMedium() throws Exception {
        dotestIndexAndQuery(random().nextInt(1000));
    }

    public void testIndexAndQueryBig() throws Exception {
        dotestIndexAndQuery(random().nextInt(10000));
    }

    public void dotestIndexAndQuery(int numPoints) throws Exception {
        IndexWriterConfig iwc = newIndexWriterConfig();
        // Else seeds may not reproduce:
        iwc.setMergeScheduler(new SerialMergeScheduler());
        // Else we can get O(N^2) merging:
        iwc.setMaxBufferedDocs(10);
        Directory dir = newDirectory();
        // RandomIndexWriter is too slow here:
        IndexWriter w = new IndexWriter(dir, iwc);
        for (int id = 0; id < numPoints; id++) {
            Document doc = new Document();
            Instant instant  = randomInstant();
            doc.add(new LongPoint("exact", instant.toEpochMilli()));
            doc.add(new SortedNumericDocValuesField("exact", instant.toEpochMilli()));
            doc.add(new LongPoint("approx", instant.getEpochSecond()));
            doc.add(new SortedNumericDocValuesField("approx", instant.toEpochMilli()));
            w.addDocument(doc);
        }

        if (random().nextBoolean()) {
            w.forceMerge(1);
        }
        final IndexReader r = DirectoryReader.open(w);
        w.close();

        IndexSearcher s = newSearcher(r);
        for ( int i = 0; i < 100; i++) {
            Instant origin  = randomInstant();
            long distance = randomLongBetween(0, 10000);

            Query q1 = LongPoint.newDistanceFeatureQuery("exact", 2f, origin.toEpochMilli(), distance);
            Query q2 = new TwoPhaseLongDistanceFeatureQuery("approx", origin.toEpochMilli(), distance, origin.getEpochSecond()) {
                @Override
                public long convertDistance(long distanceExact) {
                    return distanceExact/1000;
                }
            };
            q2 = new BoostQuery(q2, 2f);
            assertEquals(s.count(q1), s.count(q2));
            TopDocs topDocs1 = s.search(q1, 10);
            TopDocs topDocs2 = s.search(q2, 10);
            assertEquals(topDocs1.totalHits, topDocs1.totalHits);
            assertEquals(topDocs1.scoreDocs.length, topDocs2.scoreDocs.length);
            for (int j = 0; j < topDocs1.scoreDocs.length; j++) {
                assertEquals(topDocs1.scoreDocs[j].doc, topDocs2.scoreDocs[j].doc);
                assertEquals(topDocs1.scoreDocs[j].score, topDocs2.scoreDocs[j].score, 0.0);
            }
        }

        IOUtils.close(r, dir);
    }

    /**
     * @return a random instant between 1970 and ca 2065
     */
    protected Instant randomInstant() {
        return Instant.ofEpochMilli(randomLongBetween(0, 3000L));
    }

    public static long randomLongBetween(long min, long max) {
        return RandomNumbers.randomLongBetween(random(), min, max);
    }
}
