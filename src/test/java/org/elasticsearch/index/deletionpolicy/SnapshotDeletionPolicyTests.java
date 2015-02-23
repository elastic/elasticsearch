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

package org.elasticsearch.index.deletionpolicy;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.index.DirectoryReader.listCommits;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.hamcrest.Matchers.equalTo;

/**
 * A set of tests for {@link org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy}.
 */
public class SnapshotDeletionPolicyTests extends ElasticsearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    private RAMDirectory dir;
    private SnapshotDeletionPolicy deletionPolicy;
    private IndexWriter indexWriter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dir = new RAMDirectory();
        deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastDeletionPolicy(shardId, EMPTY_SETTINGS));
        indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER)
                .setIndexDeletionPolicy(deletionPolicy)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        indexWriter.close();
        dir.close();
    }

    private Document testDocument() {
        Document document = new Document();
        document.add(new TextField("test", "1", Field.Store.YES));
        return document;
    }

    @Test
    public void testSimpleSnapshot() throws Exception {
        // add a document and commit, resulting in one commit point
        indexWriter.addDocument(testDocument());
        indexWriter.commit();

        assertThat(listCommits(dir).size(), equalTo(1));

        // add another document and commit, resulting again in one commit point
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // snapshot the last commit, and then add a document and commit, now we should have two commit points
        SnapshotIndexCommit snapshot = deletionPolicy.snapshot();
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(2));

        // release the commit, add a document and commit, now we should be back to one commit point
        snapshot.close();
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));
    }

    @Test
    public void testMultiSnapshot() throws Exception {
        // add a document and commit, resulting in one commit point
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // take two snapshots
        SnapshotIndexCommit snapshot1 = deletionPolicy.snapshot();
        SnapshotIndexCommit snapshot2 = deletionPolicy.snapshot();

        // we should have two commits points
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(2));

        // release one snapshot, we should still have two commit points
        snapshot1.close();
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(2));

        // release the second snapshot, we should be back to one commit
        snapshot2.close();
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));
    }

    @Test
    public void testMultiReleaseException() throws Exception {
        // add a document and commit, resulting in one commit point
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // snapshot the last commit, and release it twice, the seconds should throw an exception
        SnapshotIndexCommit snapshot = deletionPolicy.snapshot();
        snapshot.close();
        snapshot.close();
    }

    @Test
    public void testSimpleSnapshots() throws Exception {
        // add a document and commit, resulting in one commit point
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // add another document and commit, resulting again in one commint point
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // snapshot the last commit, and then add a document and commit, now we should have two commit points
        SnapshotIndexCommit snapshot = deletionPolicy.snapshot();
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(2));

        // now, take a snapshot of all the commits
        SnapshotIndexCommits snapshots = deletionPolicy.snapshots();
        assertThat(snapshots.size(), equalTo(2));

        // release the snapshot, add a document and commit
        // we should have 3 commits points since we are holding onto the first two with snapshots
        // and we are using the keep only last
        snapshot.close();
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(3));

        // now release the snapshots, we should be back to a single commit point
        snapshots.close();
        indexWriter.addDocument(testDocument());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));
    }
}
