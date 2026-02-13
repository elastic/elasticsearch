/*
 * @notice
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
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.SuppressForbidden;
import org.junit.BeforeClass;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.lucene.store.FSDirectory.open;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class AsyncDirectIODirectoryTests extends BaseDirectoryTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @BeforeClass
    public static void checkSupported() throws IOException {
        assumeTrue(
            "This test required a JDK version that has support for ExtendedOpenOption.DIRECT",
            AsyncDirectIOIndexInput.ExtendedOpenOption_DIRECT != null
        );
        // jdk supports it, let's check that the filesystem does too
        Path path = createTempDir("directIOProbe");
        try (Directory dir = open(path); IndexOutput out = dir.createOutput("out", IOContext.DEFAULT)) {
            out.writeString("test");
        } catch (IOException e) {
            assumeNoException("test requires filesystem that supports Direct IO", e);
        }
    }

    @SuppressForbidden(reason = "requires Files.getFileStore")
    private static int getBlockSize(Path path) throws IOException {
        return Math.toIntExact(Files.getFileStore(path).getBlockSize());
    }

    @Override
    protected Directory getDirectory(Path path) throws IOException {
        return new FsDirectoryFactory.AlwaysDirectIODirectory(open(path), 8192, 8192, randomIntBetween(0, 32));
    }

    public void testIndexWriteRead() throws IOException {
        try (Directory dir = getDirectory(createTempDir("testDirectIODirectory"))) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
                Document doc = new Document();
                Field field = newField("field", "foo bar", TextField.TYPE_STORED);
                doc.add(field);

                iw.addDocument(doc);
                iw.commit();
            }

            try (IndexReader ir = DirectoryReader.open(dir)) {
                IndexSearcher s = newSearcher(ir);
                assertEquals(1, s.count(new PhraseQuery("field", "foo", "bar")));
            }
        }
    }

    public void testIllegalEOFWithFileSizeMultipleOfBlockSize() throws Exception {
        Path path = createTempDir("testIllegalEOF");
        final int fileSize = getBlockSize(path) * 2;

        try (Directory dir = getDirectory(path)) {
            IndexOutput o = dir.createOutput("out", newIOContext(random()));
            byte[] b = new byte[fileSize];
            o.writeBytes(b, 0, fileSize);
            o.close();
            try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
                i.seek(fileSize);

                // Seeking past EOF should always throw EOFException
                expectThrows(EOFException.class, () -> i.seek(fileSize + RandomizedTest.randomIntBetween(1, 2048)));

                // Reading immediately after seeking past EOF should throw EOFException
                expectThrows(EOFException.class, () -> i.readByte());
            }
        }
    }

    public void testReadPastEOFShouldThrowEOFExceptionWithEmptyFile() throws Exception {
        // fileSize needs to be 0 to test this condition. Do not randomize.
        final int fileSize = 0;
        try (Directory dir = getDirectory(createTempDir("testReadPastEOF"))) {
            try (IndexOutput o = dir.createOutput("out", newIOContext(random()))) {
                o.writeBytes(new byte[fileSize], 0, fileSize);
            }

            try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
                i.seek(fileSize);
                expectThrows(EOFException.class, () -> i.readByte());
                expectThrows(EOFException.class, () -> i.readBytes(new byte[1], 0, 1));
            }

            try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
                expectThrows(EOFException.class, () -> i.seek(fileSize + RandomizedTest.randomIntBetween(1, 2048)));
                expectThrows(EOFException.class, () -> i.readByte());
                expectThrows(EOFException.class, () -> i.readBytes(new byte[1], 0, 1));
            }

            try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
                expectThrows(EOFException.class, () -> i.readByte());
            }

            try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
                expectThrows(EOFException.class, () -> i.readBytes(new byte[1], 0, 1));
            }
        }
    }

    public void testSeekPastEOFAndRead() throws Exception {
        try (Directory dir = getDirectory(createTempDir("testSeekPastEOF"))) {
            final int len = random().nextInt(2048);

            try (IndexOutput o = dir.createOutput("out", newIOContext(random()))) {
                byte[] b = new byte[len];
                o.writeBytes(b, 0, len);
            }

            try (IndexInput i = dir.openInput("out", newIOContext(random()))) {
                // Seeking past EOF should always throw EOFException
                expectThrows(EOFException.class, () -> i.seek(len + RandomizedTest.randomIntBetween(1, 2048)));

                // Reading immediately after seeking past EOF should throw EOFException
                expectThrows(EOFException.class, () -> i.readByte());
            }
        }
    }

    // Ping-pong seeks should be really fast, since the position should be within buffer.
    // The test should complete within sub-second times, not minutes.
    public void testSeekSmall() throws IOException {
        Path tmpDir = createTempDir("testSeekSmall");
        try (Directory dir = getDirectory(tmpDir)) {
            int len = atLeast(100);
            try (IndexOutput o = dir.createOutput("out", newIOContext(random()))) {
                byte[] b = new byte[len];
                for (int i = 0; i < len; i++) {
                    b[i] = (byte) i;
                }
                o.writeBytes(b, 0, len);
            }
            try (IndexInput in = dir.openInput("out", newIOContext(random()))) {
                for (int i = 0; i < 100_000; i++) {
                    in.seek(2);
                    assertEquals(2, in.readByte());
                    in.seek(1);
                    assertEquals(1, in.readByte());
                    in.seek(0);
                    assertEquals(0, in.readByte());
                }
            }
        }
    }
}
