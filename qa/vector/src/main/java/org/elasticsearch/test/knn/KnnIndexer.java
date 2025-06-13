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
 * a copy and modification from Lucene util
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.elasticsearch.common.io.Channels;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

class KnnIndexer {
    private static final double WRITER_BUFFER_MB = 128;
    static final String ID_FIELD = "id";
    static final String VECTOR_FIELD = "vector";

    private final Path docsPath;
    private final Path indexPath;
    private final VectorEncoding vectorEncoding;
    private final int dim;
    private final VectorSimilarityFunction similarityFunction;
    private final Codec codec;
    private final int numDocs;
    private final int numIndexThreads;

    KnnIndexer(
        Path docsPath,
        Path indexPath,
        Codec codec,
        int numIndexThreads,
        VectorEncoding vectorEncoding,
        int dim,
        VectorSimilarityFunction similarityFunction,
        int numDocs
    ) {
        this.docsPath = docsPath;
        this.indexPath = indexPath;
        this.codec = codec;
        this.numIndexThreads = numIndexThreads;
        this.vectorEncoding = vectorEncoding;
        this.dim = dim;
        this.similarityFunction = similarityFunction;
        this.numDocs = numDocs;
    }

    void numSegments(KnnIndexTester.Results result) {
        try (FSDirectory dir = FSDirectory.open(indexPath); IndexReader reader = DirectoryReader.open(dir)) {
            result.numSegments = reader.leaves().size();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get segment count for index at " + indexPath, e);
        }
    }

    void createIndex(KnnIndexTester.Results result) throws IOException, InterruptedException, ExecutionException {
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCodec(codec);
        iwc.setRAMBufferSizeMB(WRITER_BUFFER_MB);
        iwc.setUseCompoundFile(false);

        iwc.setMaxFullFlushMergeWaitMillis(0);

        FieldType fieldType = switch (vectorEncoding) {
            case BYTE -> KnnByteVectorField.createFieldType(dim, similarityFunction);
            case FLOAT32 -> KnnFloatVectorField.createFieldType(dim, similarityFunction);
        };
        iwc.setInfoStream(new PrintStreamInfoStream(System.out) {
            @Override
            public boolean isEnabled(String component) {
                return Objects.equals(component, "IVF");
            }
        });
        logger.debug(
            "KnnIndexer: using codec={}, vectorEncoding={}, dim={}, similarityFunction={}",
            codec.getName(),
            vectorEncoding,
            dim,
            similarityFunction
        );

        if (Files.exists(indexPath)) {
            logger.debug("KnnIndexer: existing index at {}", indexPath);
        } else {
            Files.createDirectories(indexPath);
        }

        long start = System.nanoTime();
        try (
            FSDirectory dir = FSDirectory.open(indexPath);
            IndexWriter iw = new IndexWriter(dir, iwc);
            FileChannel in = FileChannel.open(docsPath)
        ) {
            long docsPathSizeInBytes = in.size();
            if (docsPathSizeInBytes % ((long) dim * vectorEncoding.byteSize) != 0) {
                throw new IllegalArgumentException(
                    "docsPath \"" + docsPath + "\" does not contain a whole number of vectors?  size=" + docsPathSizeInBytes
                );
            }
            logger.info(
                "docsPathSizeInBytes={}, dim={}, vectorEncoding={}, byteSize={}",
                docsPathSizeInBytes,
                dim,
                vectorEncoding,
                vectorEncoding.byteSize
            );

            VectorReader inReader = VectorReader.create(in, dim, vectorEncoding);
            try (ExecutorService exec = Executors.newFixedThreadPool(numIndexThreads, r -> new Thread(r, "KnnIndexer-Thread"))) {
                AtomicInteger numDocsIndexed = new AtomicInteger();
                List<Future<?>> threads = new ArrayList<>();
                for (int i = 0; i < numIndexThreads; i++) {
                    Thread t = new IndexerThread(iw, inReader, dim, vectorEncoding, fieldType, numDocsIndexed, numDocs);
                    t.setDaemon(true);
                    threads.add(exec.submit(t));
                }
                for (Future<?> t : threads) {
                    t.get();
                }
            }
            logger.debug("all indexing threads finished, now IndexWriter.commit()");
            iw.commit();
            ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
            cms.sync();
        }

        long elapsed = System.nanoTime() - start;
        logger.debug("Indexing took {} ms for {} docs", TimeUnit.NANOSECONDS.toMillis(elapsed), numDocs);
        result.indexTimeMS = TimeUnit.NANOSECONDS.toMillis(elapsed);
    }

    void forceMerge(KnnIndexTester.Results results) throws Exception {
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        iwc.setInfoStream(new PrintStreamInfoStream(System.out) {
            @Override
            public boolean isEnabled(String component) {
                return Objects.equals(component, "IVF");
            }
        });
        iwc.setCodec(codec);
        logger.debug("KnnIndexer: forceMerge in {}", indexPath);
        long startNS = System.nanoTime();
        try (IndexWriter iw = new IndexWriter(FSDirectory.open(indexPath), iwc)) {
            iw.forceMerge(1);
        }
        long endNS = System.nanoTime();
        long elapsedNSec = (endNS - startNS);
        logger.info("forceMerge took {} ms", TimeUnit.NANOSECONDS.toMillis(elapsedNSec));
        results.forceMergeTimeMS = TimeUnit.NANOSECONDS.toMillis(elapsedNSec);
    }

    static class IndexerThread extends Thread {
        private final IndexWriter iw;
        private final AtomicInteger numDocsIndexed;
        private final int numDocsToIndex;
        private final FieldType fieldType;
        private final VectorEncoding vectorEncoding;
        private final byte[] byteVectorBuffer;
        private final float[] floatVectorBuffer;
        private final VectorReader in;

        private IndexerThread(
            IndexWriter iw,
            VectorReader in,
            int dims,
            VectorEncoding vectorEncoding,
            FieldType fieldType,
            AtomicInteger numDocsIndexed,
            int numDocsToIndex
        ) {
            this.iw = iw;
            this.in = in;
            this.vectorEncoding = vectorEncoding;
            this.fieldType = fieldType;
            this.numDocsIndexed = numDocsIndexed;
            this.numDocsToIndex = numDocsToIndex;
            switch (vectorEncoding) {
                case BYTE -> {
                    byteVectorBuffer = new byte[dims];
                    floatVectorBuffer = null;
                }
                case FLOAT32 -> {
                    floatVectorBuffer = new float[dims];
                    byteVectorBuffer = null;
                }
                default -> throw new IllegalArgumentException("unexpected vector encoding: " + vectorEncoding);
            }
        }

        @Override
        public void run() {
            try {
                _run();
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        }

        private void _run() throws IOException {
            while (true) {
                int id = numDocsIndexed.getAndIncrement();
                if (id >= numDocsToIndex) {
                    break;
                }

                Document doc = new Document();
                switch (vectorEncoding) {
                    case BYTE -> {
                        in.next(byteVectorBuffer);
                        doc.add(new KnnByteVectorField(VECTOR_FIELD, byteVectorBuffer, fieldType));
                    }
                    case FLOAT32 -> {
                        in.next(floatVectorBuffer);
                        doc.add(new KnnFloatVectorField(VECTOR_FIELD, floatVectorBuffer, fieldType));
                    }
                }

                if ((id + 1) % 25000 == 0) {
                    logger.debug("Done indexing " + (id + 1) + " documents.");
                }
                doc.add(new StoredField(ID_FIELD, id));
                iw.addDocument(doc);
            }
        }
    }

    static class VectorReader {
        final float[] target;
        final ByteBuffer bytes;
        final FileChannel input;
        long position;

        static VectorReader create(FileChannel input, int dim, VectorEncoding vectorEncoding) throws IOException {
            int bufferSize = dim * vectorEncoding.byteSize;
            if (input.size() % ((long) dim * vectorEncoding.byteSize) != 0) {
                throw new IllegalArgumentException(
                    "vectors file \"" + input + "\" does not contain a whole number of vectors?  size=" + input.size()
                );
            }
            return new VectorReader(input, dim, bufferSize);
        }

        VectorReader(FileChannel input, int dim, int bufferSize) throws IOException {
            this.bytes = ByteBuffer.wrap(new byte[bufferSize]).order(ByteOrder.LITTLE_ENDIAN);
            this.input = input;
            this.target = new float[dim];
            reset();
        }

        void reset() throws IOException {
            position = 0;
            input.position(position);
        }

        private void readNext() throws IOException {
            int bytesRead = Channels.readFromFileChannel(this.input, position, bytes);
            if (bytesRead < bytes.capacity()) {
                position = 0;
                bytes.position(0);
                // wrap around back to the start of the file if we hit the end:
                logger.warn("VectorReader hit EOF when reading " + this.input + "; now wrapping around to start of file again");
                input.position(position);
                bytesRead = Channels.readFromFileChannel(this.input, position, bytes);
                if (bytesRead < bytes.capacity()) {
                    throw new IllegalStateException(
                        "vector file " + input + " doesn't even have enough bytes for a single vector?  got bytesRead=" + bytesRead
                    );
                }
            }
            position += bytesRead;
            bytes.position(0);
        }

        synchronized void next(float[] dest) throws IOException {
            readNext();
            bytes.asFloatBuffer().get(dest);
        }

        synchronized void next(byte[] dest) throws IOException {
            readNext();
            bytes.get(dest);
        }
    }
}
