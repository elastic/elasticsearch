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

package org.elasticsearch.benchmark.index.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeUnit;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.fs.MmapFsStore;
import org.elasticsearch.index.store.fs.NioFsStore;
import org.elasticsearch.index.store.fs.SimpleFsStore;
import org.elasticsearch.index.store.memory.ByteBufferStore;
import org.elasticsearch.index.store.memory.HeapStore;
import org.elasticsearch.index.store.ram.RamStore;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;

/**
 * @author kimchy
 */
public class SimpleStoreBenchmark {

    private final AtomicLong dynamicFilesCounter = new AtomicLong();

    private final Store store;

    private String[] staticFiles = new String[10];

    private SizeValue staticFileSize = new SizeValue(5, SizeUnit.MB);

    private SizeValue dynamicFileSize = new SizeValue(1, SizeUnit.MB);


    private int readerIterations = 10;

    private int writerIterations = 10;

    private Thread[] readerThreads = new Thread[1];

    private Thread[] writerThreads = new Thread[1];

    private CountDownLatch latch;
    private CyclicBarrier barrier1;
    private CyclicBarrier barrier2;

    public SimpleStoreBenchmark(Store store) throws Exception {
        this.store = store;
    }

    public SimpleStoreBenchmark numberStaticFiles(int numberStaticFiles) {
        this.staticFiles = new String[numberStaticFiles];
        return this;
    }

    public SimpleStoreBenchmark staticFileSize(SizeValue staticFileSize) {
        this.staticFileSize = staticFileSize;
        return this;
    }

    public SimpleStoreBenchmark dynamicFileSize(SizeValue dynamicFileSize) {
        this.dynamicFileSize = dynamicFileSize;
        return this;
    }

    public SimpleStoreBenchmark readerThreads(int readerThreads) {
        this.readerThreads = new Thread[readerThreads];
        return this;
    }

    public SimpleStoreBenchmark readerIterations(int readerIterations) {
        this.readerIterations = readerIterations;
        return this;
    }

    public SimpleStoreBenchmark writerIterations(int writerIterations) {
        this.writerIterations = writerIterations;
        return this;
    }

    public SimpleStoreBenchmark writerThreads(int writerThreads) {
        this.writerThreads = new Thread[writerThreads];
        return this;
    }

    public SimpleStoreBenchmark build() throws Exception {
        System.out.println("Creating [" + staticFiles.length + "] static files with size [" + staticFileSize + "]");
        for (int i = 0; i < staticFiles.length; i++) {
            staticFiles[i] = "static" + i;
            IndexOutput io = store.directory().createOutput(staticFiles[i]);
            for (long sizeCounter = 0; sizeCounter < staticFileSize.bytes(); sizeCounter++) {
                io.writeByte((byte) 1);
            }
            io.close();
        }
        System.out.println("Using [" + dynamicFileSize + "] size for dynamic files");

        // warmp
        StopWatch stopWatch = new StopWatch("warmup");
        stopWatch.start();
        for (String staticFile : staticFiles) {
            IndexInput ii = store.directory().openInput(staticFile);
            // do a full read
            for (long counter = 0; counter < ii.length(); counter++) {
                byte result = ii.readByte();
                if (result != 1) {
                    System.out.println("Failure, read wrong value [" + result + "]");
                }
            }
            // do a list of the files
            store.directory().listAll();
        }
        stopWatch.stop();
        System.out.println("Warmup Took: " + stopWatch.shortSummary());

        for (int i = 0; i < readerThreads.length; i++) {
            readerThreads[i] = new Thread(new ReaderThread(), "Reader[" + i + "]");
        }
        for (int i = 0; i < writerThreads.length; i++) {
            writerThreads[i] = new Thread(new WriterThread(), "Writer[" + i + "]");
        }

        latch = new CountDownLatch(readerThreads.length + writerThreads.length);
        barrier1 = new CyclicBarrier(readerThreads.length + writerThreads.length + 1);
        barrier2 = new CyclicBarrier(readerThreads.length + writerThreads.length + 1);

        return this;
    }

    public void run() throws Exception {
        for (int i = 0; i < 3; i++) {
            System.gc();
            MILLISECONDS.sleep(100);
        }

        long emptyUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();

        System.out.println("Running:");
        System.out.println("   -- Readers [" + readerThreads.length + "] with [" + readerIterations + "] iterations");
        System.out.println("   -- Writers [" + writerThreads.length + "] with [" + writerIterations + "] iterations");
        for (Thread t : readerThreads) {
            t.start();
        }
        for (Thread t : writerThreads) {
            t.start();
        }
        barrier1.await();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        barrier2.await();

        latch.await();
        stopWatch.stop();

        System.out.println("Took: " + stopWatch.shortSummary());

        for (int i = 0; i < 3; i++) {
            System.gc();
            MILLISECONDS.sleep(100);
        }
        long bytesTaken = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - emptyUsed;
        System.out.println("Size of [" + staticFiles.length + "], each with size [" + staticFileSize + "], is " + new SizeValue(bytesTaken, SizeUnit.BYTES));
    }

    private class ReaderThread implements Runnable {
        @Override public void run() {
            try {
                barrier1.await();
                barrier2.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                for (int i = 0; i < readerIterations; i++) {
                    for (String staticFile : staticFiles) {
                        // do a list of the files
                        store.directory().listAll();

                        IndexInput ii = store.directory().openInput(staticFile);
                        // do a full read
                        for (long counter = 0; counter < ii.length(); counter++) {
                            byte result = ii.readByte();
                            if (result != 1) {
                                System.out.println("Failure, read wrong value [" + result + "]");
                            }
                        }
                        // do a list of the files
                        store.directory().listAll();

                        // do a seek and read some byes
                        ii.seek(ii.length() / 2);
                        ii.readByte();
                        ii.readByte();

                        // do a list of the files
                        store.directory().listAll();
                    }
                }
            } catch (Exception e) {
                System.out.println("Reader Thread failed: " + e.getMessage());
                e.printStackTrace();
            }
            latch.countDown();
        }
    }

    private class WriterThread implements Runnable {
        @Override public void run() {
            try {
                barrier1.await();
                barrier2.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                for (int i = 0; i < writerIterations; i++) {
                    String dynamicFileName = "dynamic" + dynamicFilesCounter.incrementAndGet();
                    IndexOutput io = store.directory().createOutput(dynamicFileName);
                    for (long sizeCounter = 0; sizeCounter < dynamicFileSize.bytes(); sizeCounter++) {
                        io.writeByte((byte) 1);
                    }
                    io.close();

                    store.directory().deleteFile(dynamicFileName);
                }
            } catch (Exception e) {
                System.out.println("Writer thread failed: " + e.getMessage());
                e.printStackTrace();
            }
            latch.countDown();
        }
    }

    public static void main(String[] args) throws Exception {
        Environment environment = new Environment();
        Settings settings = EMPTY_SETTINGS;
        String localNodeId = "nodeId";
        ShardId shardId = new ShardId(new Index("index"), 1);
        String type = args.length > 0 ? args[0] : "ram";
        Store store;
        if (type.equalsIgnoreCase("ram")) {
            store = new RamStore(shardId, settings);
        } else if (type.equalsIgnoreCase("simple-fs")) {
            store = new SimpleFsStore(shardId, settings, environment, localNodeId);
        } else if (type.equalsIgnoreCase("mmap-fs")) {
            store = new NioFsStore(shardId, settings, environment, localNodeId);
        } else if (type.equalsIgnoreCase("nio-fs")) {
            store = new MmapFsStore(shardId, settings, environment, localNodeId);
        } else if (type.equalsIgnoreCase("memory-direct")) {
            Settings byteBufferSettings = settingsBuilder()
                    .put(settings)
                    .put("index.store.bytebuffer.direct", true)
                    .build();
            store = new ByteBufferStore(shardId, byteBufferSettings);
        } else if (type.equalsIgnoreCase("memory-heap")) {
            Settings memorySettings = settingsBuilder()
                    .put(settings)
                    .build();
            store = new HeapStore(shardId, memorySettings);
        } else {
            throw new IllegalArgumentException("No type store [" + type + "]");
        }
        System.out.println("Using Store [" + store + "]");
        store.deleteContent();
        SimpleStoreBenchmark simpleStoreBenchmark = new SimpleStoreBenchmark(store)
                .numberStaticFiles(5).staticFileSize(new SizeValue(5, SizeUnit.MB))
                .dynamicFileSize(new SizeValue(1, SizeUnit.MB))
                .readerThreads(5).readerIterations(10)
                .writerThreads(2).writerIterations(10)
                .build();
        simpleStoreBenchmark.run();
        store.close();
    }
}
