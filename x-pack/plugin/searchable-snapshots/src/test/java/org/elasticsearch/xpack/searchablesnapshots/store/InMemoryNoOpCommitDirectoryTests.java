/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class InMemoryNoOpCommitDirectoryTests extends ESTestCase {

    private ByteBuffersDirectory readOnlyDirectory;
    private InMemoryNoOpCommitDirectory inMemoryNoOpCommitDirectory;

    @Before
    public void createDirectories() {
        readOnlyDirectory = new ByteBuffersDirectory(NoLockFactory.INSTANCE);
        inMemoryNoOpCommitDirectory = new InMemoryNoOpCommitDirectory(new FilterDirectory(readOnlyDirectory) {
            // wrapper around readOnlyDirectory to assert that we make no attempt to write to it

            @Override
            public void deleteFile(String name) {
                throw new AssertionError("not supported");
            }

            @Override
            public IndexOutput createOutput(String name, IOContext context) {
                throw new AssertionError("not supported");
            }

            @Override
            public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
                throw new AssertionError("not supported");
            }

            @Override
            public void rename(String source, String dest) {
                throw new AssertionError("not supported");
            }

            @Override
            public Lock obtainLock(String name) {
                throw new AssertionError("not supported");
            }

            @Override
            public Set<String> getPendingDeletions() {
                throw new AssertionError("not supported");
            }

            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) {
                throw new AssertionError("not supported");
            }
        });
    }

    @After
    public void closeDirectories() throws IOException {
        inMemoryNoOpCommitDirectory.close();
        expectThrows(AlreadyClosedException.class, () -> readOnlyDirectory.listAll());
    }

    public void testAllowsWritingSegmentsFiles() throws IOException {
        assertCanWrite("segments_" + randomAlphaOfLength(10));
        assertCanWrite("pending_segments_" + randomAlphaOfLength(10));
        assertCanWrite("recovery." + randomAlphaOfLength(10) + ".segments_" + randomAlphaOfLength(10));
    }

    public void testForbidsWritingOtherFiles() {
        expectThrows(IllegalArgumentException.class, () -> assertCanWrite("not_a_segments_file"));
    }

    private void assertCanWrite(String name) throws IOException {
        final String s = randomAlphaOfLength(10);
        try (IndexOutput output = inMemoryNoOpCommitDirectory.createOutput(name, IOContext.DEFAULT)) {
            output.writeString(s);
        }

        try (IndexInput input = inMemoryNoOpCommitDirectory.openInput(name, IOContext.DEFAULT)) {
            assertThat(input.readString(), equalTo(s));
        }

        if (randomBoolean()) {
            inMemoryNoOpCommitDirectory.sync(singletonList(name));
        }

        if (randomBoolean()) {
            inMemoryNoOpCommitDirectory.syncMetaData();
        }

        assertThat(inMemoryNoOpCommitDirectory.fileLength(name), equalTo((long) StandardCharsets.UTF_8.encode(s).array().length));

        assertThat(Arrays.asList(inMemoryNoOpCommitDirectory.listAll()), hasItem(name));

        inMemoryNoOpCommitDirectory.deleteFile(name);

        assertThat(Arrays.asList(inMemoryNoOpCommitDirectory.listAll()), not(hasItem(name)));
    }

    public void testExposesFileFromRealDirectory() throws IOException {
        final String name = randomAlphaOfLength(10);
        assertExposesRealFiles(name);
        expectThrows(IllegalArgumentException.class, () -> inMemoryNoOpCommitDirectory.deleteFile(name));
        assertThat(Arrays.asList(inMemoryNoOpCommitDirectory.listAll()), hasItem(name));
    }

    public void testEmulatesAttemptsToDeleteInnerSegmentsFiles() throws IOException {
        final String name = "segments_" + randomAlphaOfLength(10);
        assertExposesRealFiles(name);
        inMemoryNoOpCommitDirectory.deleteFile(name); // no-op
        assertThat(Arrays.asList(readOnlyDirectory.listAll()), hasItem(name));
        assertThat(Arrays.asList(inMemoryNoOpCommitDirectory.listAll()), not(hasItem(name)));
        readOnlyDirectory.deleteFile(name);
        assertThat(Arrays.asList(readOnlyDirectory.listAll()), not(hasItem(name)));
        assertThat(Arrays.asList(inMemoryNoOpCommitDirectory.listAll()), not(hasItem(name)));
    }

    private void assertExposesRealFiles(String name) throws IOException {
        final String s = randomAlphaOfLength(10);

        try (IndexOutput output = readOnlyDirectory.createOutput(name, IOContext.DEFAULT)) {
            output.writeString(s);
        }

        try (IndexInput input = inMemoryNoOpCommitDirectory.openInput(name, IOContext.DEFAULT)) {
            assertThat(input.readString(), equalTo(s));
        }

        assertThat(inMemoryNoOpCommitDirectory.fileLength(name), equalTo((long) StandardCharsets.UTF_8.encode(s).array().length));

        assertThat(Arrays.asList(inMemoryNoOpCommitDirectory.listAll()), hasItem(name));
    }

    public void testSupportsNoOpCommits() throws IOException {
        try (IndexWriter indexWriter = new IndexWriter(readOnlyDirectory, new IndexWriterConfig())) {
            final Document document = new Document();
            document.add(new TextField("foo", "bar", Field.Store.YES));
            indexWriter.addDocument(document);
            indexWriter.setLiveCommitData(singletonMap("user_data", "original").entrySet());
            indexWriter.commit();
        }

        try (DirectoryReader directoryReader = DirectoryReader.open(inMemoryNoOpCommitDirectory)) {
            assertThat(directoryReader.getIndexCommit().getUserData().get("user_data"), equalTo("original"));
            final TopDocs topDocs = new IndexSearcher(directoryReader).search(new MatchAllDocsQuery(), 1);
            assertThat(topDocs.totalHits, equalTo(new TotalHits(1L, TotalHits.Relation.EQUAL_TO)));
            assertThat(topDocs.scoreDocs.length, equalTo(1));
            assertThat(directoryReader.document(topDocs.scoreDocs[0].doc).getField("foo").stringValue(), equalTo("bar"));
        }

        try (IndexWriter indexWriter = new IndexWriter(inMemoryNoOpCommitDirectory, new IndexWriterConfig())) {
            indexWriter.setLiveCommitData(singletonMap("user_data", "updated").entrySet());
            indexWriter.commit();
        }

        try (DirectoryReader directoryReader = DirectoryReader.open(inMemoryNoOpCommitDirectory)) {
            assertThat(directoryReader.getIndexCommit().getUserData().get("user_data"), equalTo("updated"));
        }
    }

    public void testRejectsDocumentChanges() throws IOException {
        if (randomBoolean()) {
            try (IndexWriter indexWriter = new IndexWriter(readOnlyDirectory, new IndexWriterConfig())) {
                final Document document = new Document();
                document.add(new TextField("foo", "bar", Field.Store.YES));
                indexWriter.addDocument(document);
                indexWriter.commit();
            }
        }

        try (IndexWriter indexWriter = new IndexWriter(inMemoryNoOpCommitDirectory, new IndexWriterConfig())) {
            final Document document = new Document();
            document.add(new TextField("foo", "baz", Field.Store.YES));
            expectThrows(IllegalArgumentException.class, () -> {
                indexWriter.addDocument(document);
                indexWriter.commit();
            });
        }
    }

    public void testSupportsDeletes() throws IOException {
        try (IndexWriter indexWriter = new IndexWriter(readOnlyDirectory, new IndexWriterConfig())) {
            final Document document = new Document();
            document.add(new TextField("foo", "bar", Field.Store.YES));
            indexWriter.addDocument(document);
            indexWriter.setLiveCommitData(singletonMap("user_data", "original").entrySet());
            indexWriter.commit();
        }

        try (DirectoryReader directoryReader = DirectoryReader.open(inMemoryNoOpCommitDirectory)) {
            assertThat(directoryReader.getIndexCommit().getUserData().get("user_data"), equalTo("original"));
            final TopDocs topDocs = new IndexSearcher(directoryReader).search(new MatchAllDocsQuery(), 1);
            assertThat(topDocs.totalHits, equalTo(new TotalHits(1L, TotalHits.Relation.EQUAL_TO)));
            assertThat(topDocs.scoreDocs.length, equalTo(1));
            assertThat(directoryReader.document(topDocs.scoreDocs[0].doc).getField("foo").stringValue(), equalTo("bar"));
        }

        assertEquals(1, DirectoryReader.listCommits(inMemoryNoOpCommitDirectory).size());

        try (
            IndexWriter indexWriter = new IndexWriter(
                inMemoryNoOpCommitDirectory,
                new IndexWriterConfig().setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE)
            )
        ) {
            indexWriter.setLiveCommitData(singletonMap("user_data", "updated").entrySet());
            indexWriter.commit();
        }

        assertEquals(2, DirectoryReader.listCommits(inMemoryNoOpCommitDirectory).size());

        try (IndexWriter indexWriter = new IndexWriter(inMemoryNoOpCommitDirectory, new IndexWriterConfig())) {
            indexWriter.commit();
        }

        assertEquals(1, DirectoryReader.listCommits(inMemoryNoOpCommitDirectory).size());

        try (DirectoryReader directoryReader = DirectoryReader.open(inMemoryNoOpCommitDirectory)) {
            assertThat(directoryReader.getIndexCommit().getUserData().get("user_data"), equalTo("updated"));
        }
    }

}
