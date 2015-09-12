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
package org.elasticsearch.benchmark.fs;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 *
 */
public class FsAppendBenchmark {

    public static void main(String[] args) throws Exception {
        Path path = PathUtils.get("work/test.log");
        IOUtils.deleteFilesIgnoringExceptions(path);

        int CHUNK = (int) ByteSizeValue.parseBytesSizeValue("1k", "CHUNK").bytes();
        long DATA = ByteSizeValue.parseBytesSizeValue("10gb", "DATA").bytes();

        byte[] data = new byte[CHUNK];
        new Random().nextBytes(data);

        StopWatch watch = new StopWatch().start("write");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            long position = 0;
            while (position < DATA) {
                channel.write(ByteBuffer.wrap(data), position);
                position += data.length;
            }
            watch.stop().start("flush");
            channel.force(true);
        }
        watch.stop();
        System.out.println("Wrote [" + (new ByteSizeValue(DATA)) + "], chunk [" + (new ByteSizeValue(CHUNK)) + "], in " + watch);
    }

    private static final ByteBuffer fill = ByteBuffer.allocateDirect(1);

//    public static long padLogFile(long position, long currentSize, long preAllocSize) throws IOException {
//        if (position + 4096 >= currentSize) {
//            currentSize = currentSize + preAllocSize;
//            fill.position(0);
//            f.getChannel().write(fill, currentSize - fill.remaining());
//        }
//        return currentSize;
//    }
}
