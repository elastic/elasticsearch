/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.common.compress;

import jsr166y.ThreadLocalRandom;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.RandomStringGenerator;
import org.elasticsearch.common.compress.CompressedDirectory;
import org.elasticsearch.common.compress.CompressedIndexInput;
import org.elasticsearch.common.compress.CompressedIndexOutput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.lzf.LZFCompressor;
import org.elasticsearch.common.compress.snappy.xerial.XerialSnappy;
import org.elasticsearch.common.compress.snappy.xerial.XerialSnappyCompressor;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.SizeValue;
import org.testng.annotations.Test;

import java.io.EOFException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Test
public class CompressIndexInputOutputTests {

    @Test
    public void testXerialSnappy() throws Exception {
        if (XerialSnappy.available) {
            testCompressor(new XerialSnappyCompressor());
        }
    }

    @Test
    public void testLZF() throws Exception {
        testCompressor(new LZFCompressor());
    }

    @Test
    public void testSideAffects() throws Exception {
        if (XerialSnappy.available) {
            testCompressor(new XerialSnappyCompressor());
        }
        testCompressor(new LZFCompressor());
        if (XerialSnappy.available) {
            testCompressor(new XerialSnappyCompressor());
        }
        testCompressor(new LZFCompressor());
    }

    private void testCompressor(Compressor compressor) throws Exception {
        empty(compressor);
        simple(compressor);
        seek1Compressed(compressor);
        seek1UnCompressed(compressor);
        copyBytes(compressor);
        lucene(compressor);
    }

    private void empty(Compressor compressor) throws Exception {
        Directory dir = new RAMDirectory();
        IndexOutput out = compressor.indexOutput(dir.createOutput("test"));
        out.close();

        IndexInput in = compressor.indexInput(dir.openInput("test"));
        try {
            in.readByte();
            assert false;
        } catch (EOFException e) {
            // all is well
        }

        in.seek(100);
        try {
            in.readByte();
            assert false;
        } catch (EOFException e) {
            // all is well
        }
    }

    private void simple(Compressor compressor) throws Exception {
        Directory dir = new RAMDirectory();
        IndexOutput out = compressor.indexOutput(dir.createOutput("test"));
        long pos1 = out.getFilePointer();
        out.writeInt(1);
        long pos2 = out.getFilePointer();
        out.writeString("test1");
        long pos3 = out.getFilePointer();
        String largeString = RandomStringGenerator.random(0xFFFF + 5);
        out.writeString(largeString);
        long pos4 = out.getFilePointer();
        out.writeInt(2);
        long pos5 = out.getFilePointer();
        out.writeString("test2");
        out.close();

        IndexInput in = compressor.indexInput(dir.openInput("test"));
        assertThat(in.readInt(), equalTo(1));
        assertThat(in.readString(), equalTo("test1"));
        assertThat(in.readString(), equalTo(largeString));
        assertThat(in.readInt(), equalTo(2));
        assertThat(in.readString(), equalTo("test2"));

        in.seek(pos3);
        assertThat(in.readString(), equalTo(largeString));
        in.seek(pos2);
        assertThat(in.readString(), equalTo("test1"));
        in.seek(pos5);
        assertThat(in.readString(), equalTo("test2"));
        in.seek(pos1);
        assertThat(in.readInt(), equalTo(1));

        in.seek(0);
        byte[] full = new byte[(int) in.length()];
        in.readBytes(full, 0, full.length);

        in.close();
    }

    private void seek1Compressed(Compressor compressor) throws Exception {
        seek1(true, compressor);
    }

    private void seek1UnCompressed(Compressor compressor) throws Exception {
        seek1(false, compressor);
    }

    private void seek1(boolean compressed, Compressor compressor) throws Exception {
        Directory dir = new RAMDirectory();
        IndexOutput out = compressed ? compressor.indexOutput(dir.createOutput("test")) : dir.createOutput("test");
        long pos1 = out.getFilePointer();
        out.writeVInt(4);
        out.writeInt(1);
        long pos2 = out.getFilePointer();
        out.writeVInt(8);
        long posX = out.getFilePointer();
        out.writeInt(2);
        out.writeInt(3);
        long pos3 = out.getFilePointer();
        out.writeVInt(4);
        out.writeInt(4);

        int size = 50;
        long[] positions = new long[size];
        String[] data = new String[size];
        for (int i = 0; i < 50; i++) {
            positions[i] = out.getFilePointer();
            data[i] = RandomStringGenerator.random(12345);
            out.writeString(data[i]);
        }

        out.close();

        //IndexInput in = dir.openInput("test");
        IndexInput in = compressed ? compressor.indexInput(dir.openInput("test")) : dir.openInput("test");
        in.seek(pos2);
        // now "skip"
        int numBytes = in.readVInt();
        assertThat(in.getFilePointer(), equalTo(posX));
        in.seek(in.getFilePointer() + numBytes);
        assertThat(in.readVInt(), equalTo(4));
        assertThat(in.readInt(), equalTo(4));

        for (int i = 0; i < size; i++) {
            in.seek(positions[i]);
            assertThat(in.getFilePointer(), equalTo(positions[i]));
            assertThat(in.readString(), equalTo(data[i]));
        }
    }

    private void copyBytes(Compressor compressor) throws Exception {
        Directory dir = new RAMDirectory();
        IndexOutput out = compressor.indexOutput(dir.createOutput("test"));
        long pos1 = out.getFilePointer();
        out.writeInt(1);
        long pos2 = out.getFilePointer();
        assertThat(pos2, equalTo(4l));
        out.writeString("test1");
        long pos3 = out.getFilePointer();
        String largeString = RandomStringGenerator.random(0xFFFF + 5);
        out.writeString(largeString);
        long pos4 = out.getFilePointer();
        out.writeInt(2);
        long pos5 = out.getFilePointer();
        out.writeString("test2");
        assertThat(out.length(), equalTo(out.getFilePointer()));
        long length = out.length();
        out.close();

        CompressedIndexOutput out2 = compressor.indexOutput(dir.createOutput("test2"));
        out2.writeString("mergeStart");
        long startMergePos = out2.getFilePointer();
        CompressedIndexInput testInput = compressor.indexInput(dir.openInput("test"));
        assertThat(testInput.length(), equalTo(length));
        out2.copyBytes(testInput, testInput.length());
        long endMergePos = out2.getFilePointer();
        out2.writeString("mergeEnd");
        out2.close();

        IndexInput in = compressor.indexInput(dir.openInput("test2"));
        assertThat(in.readString(), equalTo("mergeStart"));
        assertThat(in.readInt(), equalTo(1));
        assertThat(in.readString(), equalTo("test1"));
        assertThat(in.readString(), equalTo(largeString));
        assertThat(in.readInt(), equalTo(2));
        assertThat(in.readString(), equalTo("test2"));
        assertThat(in.readString(), equalTo("mergeEnd"));

        in.seek(pos1);
        assertThat(in.readString(), equalTo("mergeStart"));
        in.seek(endMergePos);
        assertThat(in.readString(), equalTo("mergeEnd"));

        try {
            in.readByte();
            assert false;
        } catch (EOFException e) {
            // all is well, we reached hte end...
        }
    }

    private void lucene(Compressor compressor) throws Exception {
        CompressedDirectory dir = new CompressedDirectory(new RAMDirectory(), compressor, false, "fdt");
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));
        writer.addDocument(createDoc(1, (int) SizeValue.parseSizeValue("100b").singles()));
        writer.addDocument(createDoc(2, (int) SizeValue.parseSizeValue("5k").singles()));
        writer.commit();
        writer.addDocument(createDoc(3, (int) SizeValue.parseSizeValue("2k").singles()));
        writer.addDocument(createDoc(4, (int) SizeValue.parseSizeValue("1k").singles()));
        writer.commit();
        verify(writer);
        writer.forceMerge(1);
        writer.waitForMerges();
        verify(writer);
        dir.setCompress(false);
        writer.addDocument(createDoc(5, (int) SizeValue.parseSizeValue("2k").singles()));
        writer.addDocument(createDoc(6, (int) SizeValue.parseSizeValue("1k").singles()));
        verify(writer);
        writer.forceMerge(1);
        writer.waitForMerges();
        verify(writer);
        writer.close();
    }

    private void verify(IndexWriter writer) throws Exception {
        CheckIndex checkIndex = new CheckIndex(writer.getDirectory());
        CheckIndex.Status status = checkIndex.checkIndex();
        assertThat(status.clean, equalTo(true));
        IndexReader reader = IndexReader.open(writer, true);
        for (int i = 0; i < reader.maxDoc(); i++) {
            if (reader.isDeleted(i)) {
                continue;
            }
            Document document = reader.document(i);
            checkDoc(document);
            document = reader.document(i, new MapFieldSelector("id", "field", "count"));
            checkDoc(document);
        }
        for (int i = 0; i < 100; i++) {
            int doc = ThreadLocalRandom.current().nextInt(reader.maxDoc());
            if (reader.isDeleted(i)) {
                continue;
            }
            Document document = reader.document(doc);
            checkDoc(document);
            document = reader.document(doc, new MapFieldSelector("id", "field", "count"));
            checkDoc(document);
        }
    }

    private void checkDoc(Document document) {
        String id = document.get("id");
        String field = document.get("field");
        int count = 0;
        int idx = 0;
        while (true) {
            int oldIdx = idx;
            idx = field.indexOf(' ', oldIdx);
            if (idx == -1) {
                break;
            }
            count++;
            assertThat(field.substring(oldIdx, idx), equalTo(id));
            idx++;
        }
        assertThat(count, equalTo(Integer.parseInt(document.get("count"))));
    }

    private Document createDoc(int id, int size) {
        Document doc = new Document();
        doc.add(new Field("id", Integer.toString(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("size", Integer.toString(size), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("skip", RandomStringGenerator.random(50), Field.Store.YES, Field.Index.NO));
        StringBuilder sb = new StringBuilder();
        int count = 0;
        while (true) {
            count++;
            sb.append(id);
            sb.append(" ");
            if (sb.length() >= size) {
                break;
            }
        }
        doc.add(new Field("count", Integer.toString(count), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("field", sb.toString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("skip", RandomStringGenerator.random(50), Field.Store.YES, Field.Index.NO));
        return doc;
    }
}
