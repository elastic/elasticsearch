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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.TwoPhaseDatePointMillis;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

import java.time.Instant;

public class TwoPhaseDateTests extends LuceneTestCase {

    public void testIndexMillis() throws Exception {
        IndexWriterConfig iwc = newIndexWriterConfig();
        // Else seeds may not reproduce:
        iwc.setMergeScheduler(new SerialMergeScheduler());
        // Else we can get O(N^2) merging:
        iwc.setMaxBufferedDocs(10);
        Directory dir = newDirectory();
        // RandomIndexWriter is too slow here:
        int numPoints = random().nextInt(10000);
        IndexWriter w = new IndexWriter(dir, iwc);
        for (int id = 0; id < numPoints; id++) {
            Document doc = new Document();
            Instant instant  = randomInstant();
            doc.add(new LongPoint("exact", instant.toEpochMilli()));
            Field[] fields = TwoPhaseDatePointMillis.createIndexableFields("approx", instant);
            for (int i =0; i < fields.length; i++) {
                doc.add(fields[i]);
            }
            w.addDocument(doc);
        }

        if (random().nextBoolean()) {
            w.forceMerge(1);
        }
        final IndexReader r = DirectoryReader.open(w);
        w.close();

        IndexSearcher s = newSearcher(r);
        for ( int i = 0; i < 100; i++) {
            Instant instant1  = randomInstant();
            Instant instant2  = randomInstant();
            Query q1;
            Query q2;
            if (instant1.toEpochMilli() >= instant2.toEpochMilli()) {
                q1 = LongPoint.newRangeQuery("exact", instant2.toEpochMilli(), instant1.toEpochMilli());
                q2 = TwoPhaseDatePointMillis.newRangeQuery("approx", instant2, instant1);
            } else {
                q1 = LongPoint.newRangeQuery("exact", instant1.toEpochMilli(), instant2.toEpochMilli());
                q2 = TwoPhaseDatePointMillis.newRangeQuery("approx", instant1, instant2);
            }
            assertEquals(s.count(q1), s.count(q2));
        }

        IOUtils.close(r, dir);
    }

    /**
     * @return a random instant between 1970 and ca 2065
     */
    protected Instant randomInstant() {
        //return Instant.ofEpochSecond(randomLongBetween(0, 3000000000L), randomLongBetween(0, 999999999));
        return Instant.ofEpochSecond(randomLongBetween(0, 30000L), randomLongBetween(0, 999999999));
    }

    public static long randomLongBetween(long min, long max) {
        return RandomNumbers.randomLongBetween(random(), min, max);
    }
}
