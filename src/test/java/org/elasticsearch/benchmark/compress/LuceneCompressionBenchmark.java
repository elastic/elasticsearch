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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.common.compress.CompressedIndexInput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.File;
import java.io.IOException;

/**
 */
public class LuceneCompressionBenchmark {

    public static void main(String[] args) throws Exception {
        final long MAX_SIZE = ByteSizeValue.parseBytesSizeValue("50mb").bytes();

        final Compressor compressor = CompressorFactory.defaultCompressor();

        File testFile = new File("target/test/compress/lucene");
        FileSystemUtils.deleteRecursively(testFile);
        testFile.mkdirs();

        FSDirectory uncompressedDir = new NIOFSDirectory(new File(testFile, "uncompressed"));
        IndexWriter uncompressedWriter = new IndexWriter(uncompressedDir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        FSDirectory compressedDir = new NIOFSDirectory(new File(testFile, "compressed")) {
            @Override
            public IndexOutput createOutput(String name) throws IOException {
                if (name.endsWith(".fdt")) {
                    return compressor.indexOutput(super.createOutput(name));
                }
                return super.createOutput(name);
            }

            @Override
            public IndexInput openInput(String name) throws IOException {
                if (name.endsWith(".fdt")) {
                    IndexInput in = super.openInput(name);
                    Compressor compressor1 = CompressorFactory.compressor(in);
                    if (compressor1 != null) {
                        return compressor1.indexInput(in);
                    } else {
                        return in;
                    }
                }
                return super.openInput(name);
            }

            @Override
            public IndexInput openInput(String name, int bufferSize) throws IOException {
                if (name.endsWith(".fdt")) {
                    IndexInput in = super.openInput(name, bufferSize);
                    // in case the override called openInput(String)
                    if (in instanceof CompressedIndexInput) {
                        return in;
                    }
                    Compressor compressor1 = CompressorFactory.compressor(in);
                    if (compressor1 != null) {
                        return compressor1.indexInput(in);
                    } else {
                        return in;
                    }
                }
                return super.openInput(name, bufferSize);
            }
        };

        IndexWriter compressedWriter = new IndexWriter(compressedDir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        System.out.println("feeding data...");
        TestData testData = new TestData();
        while (testData.next() && testData.getTotalSize() < MAX_SIZE) {
            // json
            XContentBuilder builder = XContentFactory.jsonBuilder();
            testData.current(builder);
            builder.close();
            Document doc = new Document();
            doc.add(new Field("_source", builder.underlyingBytes(), 0, builder.underlyingBytesLength()));
            uncompressedWriter.addDocument(doc);
            compressedWriter.addDocument(doc);
        }
        System.out.println("optimizing...");
        uncompressedWriter.forceMerge(1);
        compressedWriter.forceMerge(1);
        uncompressedWriter.waitForMerges();
        compressedWriter.waitForMerges();

        System.out.println("done");
        uncompressedDir.close();
        compressedWriter.close();

        compressedDir.close();
        uncompressedDir.close();
    }

}
