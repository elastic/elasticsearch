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

package org.elasticsearch.benchmark.compress;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.common.compress.CompressedDirectory;
import org.elasticsearch.common.compress.lzf.LZFCompressor;
import org.elasticsearch.common.compress.snappy.xerial.XerialSnappyCompressor;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.File;

/**
 */
public class LuceneCompressionBenchmark {

    public static void main(String[] args) throws Exception {
        final long MAX_SIZE = ByteSizeValue.parseBytesSizeValue("50mb").bytes();
        final boolean WITH_TV = true;

        File testFile = new File("target/test/compress/lucene");
        FileSystemUtils.deleteRecursively(testFile);
        testFile.mkdirs();

        FSDirectory uncompressedDir = new NIOFSDirectory(new File(testFile, "uncompressed"));
        IndexWriter uncompressedWriter = new IndexWriter(uncompressedDir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Directory compressedLzfDir = new CompressedDirectory(new NIOFSDirectory(new File(testFile, "compressed_lzf")), new LZFCompressor(), false, "fdt", "tvf");
        IndexWriter compressedLzfWriter = new IndexWriter(compressedLzfDir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Directory compressedSnappyDir = new CompressedDirectory(new NIOFSDirectory(new File(testFile, "compressed_snappy")), new XerialSnappyCompressor(), false, "fdt", "tvf");
        IndexWriter compressedSnappyWriter = new IndexWriter(compressedSnappyDir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        System.out.println("feeding data...");
        TestData testData = new TestData();
        while (testData.next() && testData.getTotalSize() < MAX_SIZE) {
            // json
            XContentBuilder builder = XContentFactory.jsonBuilder();
            testData.current(builder);
            builder.close();
            Document doc = new Document();
            doc.add(new Field("_source", builder.bytes().array(), builder.bytes().arrayOffset(), builder.bytes().length()));
            if (WITH_TV) {
                Field field = new Field("text", builder.string(), Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
                doc.add(field);
            }
            uncompressedWriter.addDocument(doc);
            compressedLzfWriter.addDocument(doc);
            compressedSnappyWriter.addDocument(doc);
        }
        System.out.println("optimizing...");
        uncompressedWriter.forceMerge(1);
        compressedLzfWriter.forceMerge(1);
        compressedSnappyWriter.forceMerge(1);
        uncompressedWriter.waitForMerges();
        compressedLzfWriter.waitForMerges();
        compressedSnappyWriter.waitForMerges();

        System.out.println("done");
        uncompressedWriter.close();
        compressedLzfWriter.close();
        compressedSnappyWriter.close();

        compressedLzfDir.close();
        compressedSnappyDir.close();
        uncompressedDir.close();
    }

}
