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

package org.apache.lucene;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.TrackingSerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.merge.EnableMergeScheduler;
import org.elasticsearch.index.merge.Merges;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.Test;

/**
 */
public class TrackingSerialMergeSchedulerTests extends ElasticsearchLuceneTestCase {

    @Test
    public void testMaxMergeAtOnce() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
        // create a tracking merge scheduler, but enabled one, so we can control when it merges
        EnableMergeScheduler mergeScheduler = new EnableMergeScheduler(new TrackingSerialMergeScheduler(Loggers.getLogger(getTestClass()), 2));
        iwc.setMergeScheduler(mergeScheduler);
        TieredMergePolicy mergePolicy = new TieredMergePolicy();
        mergePolicy.setMaxMergeAtOnceExplicit(3);
        mergePolicy.setMaxMergeAtOnce(3);
        iwc.setMergePolicy(mergePolicy);
        IndexWriter iw = new IndexWriter(dir, iwc);
        // create 20 segments
        for (int i = 0; i < 20; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
            iw.addDocument(doc);
            iw.commit(); // create a segment, no merge will happen, its disabled
        }
        // based on the merge policy maxMerge, and the fact that we allow only for 2 merges to run
        // per maybeMerge in our configuration of the serial merge scheduler, the we expect to need
        // 4 merge runs to work out through the pending merges
        for (int i = 0; i < 4; i++) {
            assertTrue(iw.hasPendingMerges());
            Merges.maybeMerge(iw);
            assertTrue(iw.hasPendingMerges());
        }
        Merges.maybeMerge(iw);
        assertFalse(iw.hasPendingMerges());

        iw.close(false);
        dir.close();
    }
}
