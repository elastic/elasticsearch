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

import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

/**
 *
 */
public class FsAppendBenchmark {

    public static void main(String[] args) throws Exception {
        new File("work/test.log").delete();
        RandomAccessFile raf = new RandomAccessFile("work/test.log", "rw");
        raf.setLength(0);

        boolean CHANNEL = true;
        int CHUNK = (int) ByteSizeValue.parseBytesSizeValue("1k").bytes();
        long DATA = ByteSizeValue.parseBytesSizeValue("10gb").bytes();

        byte[] data = new byte[CHUNK];
        new Random().nextBytes(data);

        StopWatch watch = new StopWatch().start("write");
        if (CHANNEL) {
            FileChannel channel = raf.getChannel();
            long position = 0;
            while (position < DATA) {
                channel.write(ByteBuffer.wrap(data), position);
                position += data.length;
            }
            watch.stop().start("flush");
            channel.force(true);
        } else {
            long position = 0;
            while (position < DATA) {
                raf.write(data);
                position += data.length;
            }
            watch.stop().start("flush");
            raf.getFD().sync();
        }
        raf.close();
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
