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

package org.elasticsearch.test.stress.compress;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.benchmark.compress.TestData;
import org.elasticsearch.common.compress.CompressedDirectory;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.joda.time.DateTime;

import java.io.File;
import java.io.PrintStream;

/**
 */
public class LuceneCompressionStressTest {

    public static void main(String[] args) throws Exception {
        final boolean USE_COMPOUND = false;
        final Compressor compressor = CompressorFactory.defaultCompressor();

        File testFile = new File("target/bench/compress/lucene");
        FileSystemUtils.deleteRecursively(testFile);
        testFile.mkdirs();


        Directory dir = new CompressedDirectory(new NIOFSDirectory(new File(testFile, "compressed")), compressor, false, "fdt", "tvf");
        TieredMergePolicy mergePolicy = new TieredMergePolicy();
        mergePolicy.setUseCompoundFile(USE_COMPOUND);
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER).setMergePolicy(mergePolicy));

        System.out.println("feeding data...");
        TestData testData = new TestData();
        long count = 0;
        long round = 0;
        while (true) {
            // json
            XContentBuilder builder = XContentFactory.jsonBuilder();
            testData.current(builder);
            builder.close();
            Document doc = new Document();
            doc.add(new Field("_source", builder.bytes().array(), builder.bytes().arrayOffset(), builder.bytes().length()));
            if (true) {
                Field field = new Field("text", builder.string(), Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
                doc.add(field);
            }
            writer.addDocument(doc);

            if ((++count % 10000) == 0) {
                writer.commit();
                ++round;
                System.out.println(DateTime.now() + "[" + round + "] closing");
                writer.close(true);
                System.out.println(DateTime.now() + "[" + round + "] closed");
                CheckIndex checkIndex = new CheckIndex(dir);
                FastByteArrayOutputStream os = new FastByteArrayOutputStream();
                PrintStream out = new PrintStream(os);
                checkIndex.setInfoStream(out);
                out.flush();
                CheckIndex.Status status = checkIndex.checkIndex();
                if (!status.clean) {
                    System.out.println("check index [failure]\n" + new String(os.bytes().toBytes()));
                } else {
                    System.out.println(DateTime.now() + "[" + round + "] checked");
                }
                mergePolicy = new TieredMergePolicy();
                mergePolicy.setUseCompoundFile(USE_COMPOUND);
                writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER).setMergePolicy(mergePolicy));
            }
        }
    }
}
