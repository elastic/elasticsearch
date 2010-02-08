/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.elasticsearch.util.lucene.Directories.*;
import static org.elasticsearch.util.lucene.DocumentBuilder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * A set of tests for {@link SnapshotDeletionPolicy}.
 *
 * @author kimchy (Shay Banon)
 */
public class SnapshotDeletionPolicyTests {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    private RAMDirectory dir;
    private SnapshotDeletionPolicy deletionPolicy;
    private IndexWriter indexWriter;

    @BeforeTest public void setUp() throws Exception {
        dir = new RAMDirectory();
        deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastDeletionPolicy(shardId, EMPTY_SETTINGS));
        indexWriter = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_CURRENT), true, deletionPolicy, IndexWriter.MaxFieldLength.UNLIMITED);
    }

    @AfterTest public void tearDown() throws Exception {
        indexWriter.close();
        dir.close();
    }

    @Test public void testSimpleSnapshot() throws Exception {
        // add a document and commit, resulting in one commit point
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();

        assertThat(listCommits(dir).size(), equalTo(1));

        // add another document and commit, resulting again in one commit point
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // snapshot the last commit, and then add a document and commit, now we should have two commit points
        SnapshotIndexCommit snapshot = deletionPolicy.snapshot();
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(2));

        // release the commit, add a document and commit, now we should be back to one commit point
        assertThat(snapshot.release(), equalTo(true));
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));
    }

    @Test public void testMultiSnapshot() throws Exception {
        // add a document and commit, resulting in one commit point
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // take two snapshots
        SnapshotIndexCommit snapshot1 = deletionPolicy.snapshot();
        SnapshotIndexCommit snapshot2 = deletionPolicy.snapshot();

        // we should have two commits points
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(2));

        // release one snapshot, we should still have two commit points
        assertThat(snapshot1.release(), equalTo(true));
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(2));

        // release the second snapshot, we should be back to one commit
        assertThat(snapshot2.release(), equalTo(true));
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));
    }

    @Test public void testMultiReleaseException() throws Exception {
        // add a document and commit, resulting in one commit point
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // snapshot the last commit, and release it twice, the seconds should throw an exception
        SnapshotIndexCommit snapshot = deletionPolicy.snapshot();
        assertThat(snapshot.release(), equalTo(true));
        assertThat(snapshot.release(), equalTo(false));
    }

    @Test public void testSimpleSnapshots() throws Exception {
        // add a document and commit, resulting in one commit point
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // add another document and commit, resulting again in one commint point
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));

        // snapshot the last commit, and then add a document and commit, now we should have two commit points
        SnapshotIndexCommit snapshot = deletionPolicy.snapshot();
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(2));

        // now, take a snapshot of all the commits
        SnapshotIndexCommits snapshots = deletionPolicy.snapshots();
        assertThat(snapshots.size(), equalTo(2));

        // release the snapshot, add a document and commit
        // we should have 3 commits points since we are holding onto the first two with snapshots
        // and we are using the keep only last
        assertThat(snapshot.release(), equalTo(true));
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(3));

        // now release the snapshots, we should be back to a single commit point
        assertThat(snapshots.release(), equalTo(true));
        indexWriter.addDocument(doc().add(field("test", "1")).build());
        indexWriter.commit();
        assertThat(listCommits(dir).size(), equalTo(1));
    }
}
