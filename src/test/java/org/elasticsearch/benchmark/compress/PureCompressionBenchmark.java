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

import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.File;
import java.io.FileOutputStream;

/**
 */
public class PureCompressionBenchmark {

    public static void main(String[] args) throws Exception {

        final long MAX_SIZE = ByteSizeValue.parseBytesSizeValue("50mb").bytes();

        File testFile = new File("target/test/compress/pure");
        FileSystemUtils.deleteRecursively(testFile);
        testFile.mkdirs();

        FileOutputStream rawJson = new FileOutputStream(new File(testFile, "raw_json"));
        FileOutputStream rawSmile = new FileOutputStream(new File(testFile, "raw_smile"));

        FileOutputStream compressedByDocJson = new FileOutputStream(new File(testFile, "compressed_by_doc_json"));
        FileOutputStream compressedByDocSmile = new FileOutputStream(new File(testFile, "compressed_by_doc_smile"));

        Compressor compressor = CompressorFactory.defaultCompressor();

        StreamOutput compressedJson = compressor.streamOutput(new OutputStreamStreamOutput(new FileOutputStream(new File(testFile, "compressed_json"))));
        StreamOutput compressedSmile = compressor.streamOutput(new OutputStreamStreamOutput(new FileOutputStream(new File(testFile, "compressed_smile"))));

        TestData testData = new TestData();
        while (testData.next() && testData.getTotalSize() < MAX_SIZE) {
            {
                // json
                XContentBuilder builder = XContentFactory.jsonBuilder();
                testData.current(builder);

                rawJson.write(builder.underlyingBytes(), 0, builder.underlyingBytesLength());
                compressedJson.write(builder.underlyingBytes(), 0, builder.underlyingBytesLength());

                byte[] compressed = compressor.compress(builder.underlyingBytes(), 0, builder.underlyingBytesLength());
                compressedByDocJson.write(compressed);
                builder.close();
            }

            {
                // smile
                XContentBuilder builder = XContentFactory.smileBuilder();
                testData.current(builder);

                rawSmile.write(builder.underlyingBytes(), 0, builder.underlyingBytesLength());
                compressedSmile.write(builder.underlyingBytes(), 0, builder.underlyingBytesLength());

                byte[] compressed = compressor.compress(builder.underlyingBytes(), 0, builder.underlyingBytesLength());
                compressedByDocSmile.write(compressed);
                builder.close();
            }
        }

        rawJson.close();
        rawSmile.close();
        compressedJson.close();
        compressedSmile.close();
        compressedByDocJson.close();
        compressedByDocSmile.close();
    }
}
