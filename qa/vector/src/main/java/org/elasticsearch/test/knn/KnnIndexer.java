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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.StandardIOBehaviorHint;
import org.elasticsearch.index.store.FsDirectoryFactory;

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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

class KnnIndexer {
    static final String ID_FIELD = "id";
    static final String VECTOR_FIELD = "vector";

    private final List<Path> docsPath;
    private final Path indexPath;
    private final VectorEncoding vectorEncoding;
    private int dim;
    private final VectorSimilarityFunction similarityFunction;
    private final Codec codec;
    private final int numDocs;
    private final int numIndexThreads;
    private final MergePolicy mergePolicy;
    private final double writerBufferSizeInMb;
    private final int writerMaxBufferedDocs;

    KnnIndexer(
        List<Path> docsPath,
        Path indexPath,
        Codec codec,
        int numIndexThreads,
        VectorEncoding vectorEncoding,
        int dim,
        VectorSimilarityFunction similarityFunction,
        int numDocs,
        MergePolicy mergePolicy,
        double writerBufferSizeInMb,
        int writerMaxBufferedDocs
    ) {
        this.docsPath = docsPath;
        this.indexPath = indexPath;
        this.codec = codec;
        this.numIndexThreads = numIndexThreads;
        this.vectorEncoding = vectorEncoding;
        this.dim = dim;
        this.similarityFunction = similarityFunction;
        this.numDocs = numDocs;
        this.mergePolicy = mergePolicy;
        this.writerBufferSizeInMb = writerBufferSizeInMb;
        this.writerMaxBufferedDocs = writerMaxBufferedDocs;
    }

    void createIndex(KnnIndexTester.Results result) throws IOException, InterruptedException, ExecutionException {
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCodec(codec);
        iwc.setMaxBufferedDocs(writerMaxBufferedDocs);
        iwc.setRAMBufferSizeMB(writerBufferSizeInMb);
        iwc.setUseCompoundFile(false);
        if (mergePolicy != null) {
            iwc.setMergePolicy(mergePolicy);
        }
        iwc.setMaxFullFlushMergeWaitMillis(0);

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
        AtomicInteger numDocsIndexed = new AtomicInteger();
        try (Directory dir = getDirectory(indexPath); IndexWriter iw = new IndexWriter(dir, iwc)) {
            for (Path docsPath : this.docsPath) {
                int dim = this.dim;
                try (FileChannel in = FileChannel.open(docsPath)) {
                    long docsPathSizeInBytes = in.size();
                    int offsetByteSize = 0;
                    if (dim == -1) {
                        offsetByteSize = 4;
                        ByteBuffer preamble = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                        int bytesRead = Channels.readFromFileChannel(in, 0, preamble);
                        if (bytesRead < 4) {
                            throw new IllegalArgumentException(
                                "docsPath \"" + docsPath + "\" does not contain a valid dims?  size=" + docsPathSizeInBytes
                            );
                        }
                        dim = preamble.getInt(0);
                        if (dim <= 0) {
                            throw new IllegalArgumentException("docsPath \"" + docsPath + "\" has invalid dimension: " + dim);
                        }
                    }
                    FieldType fieldType = switch (vectorEncoding) {
                        case BYTE -> KnnByteVectorField.createFieldType(dim, similarityFunction);
                        case FLOAT32 -> KnnFloatVectorField.createFieldType(dim, similarityFunction);
                    };
                    if (docsPathSizeInBytes % (((long) dim * vectorEncoding.byteSize + offsetByteSize)) != 0) {
                        throw new IllegalArgumentException(
                            "docsPath \"" + docsPath + "\" does not contain a whole number of vectors?  size=" + docsPathSizeInBytes
                        );
                    }
                    int numDocs = (int) (docsPathSizeInBytes / ((long) dim * vectorEncoding.byteSize + offsetByteSize));
                    numDocs = Math.min(this.numDocs - numDocsIndexed.get(), numDocs);
                    if (numDocs <= 0) {
                        break;
                    }
                    logger.info(
                        "path={}, docsPathSizeInBytes={}, numDocs={}, dim={}, vectorEncoding={}, byteSize={}",
                        docsPath,
                        docsPathSizeInBytes,
                        numDocs,
                        dim,
                        vectorEncoding,
                        vectorEncoding.byteSize
                    );
                    // adjust numDocs to account for the number of documents already indexed
                    // numDocsIndexed tracks the total docs read in order and is used for docIds
                    // numDocs is the total number of docs to index from this file
                    numDocs += numDocsIndexed.get();

                    VectorReader inReader = VectorReader.create(in, dim, vectorEncoding, offsetByteSize);
                    try (ExecutorService exec = Executors.newFixedThreadPool(numIndexThreads, r -> new Thread(r, "KnnIndexer-Thread"))) {
                        List<Future<?>> futures = new ArrayList<>();
                        List<IndexerThread> threads = new ArrayList<>();
                        for (int i = 0; i < numIndexThreads; i++) {
                            var t = new IndexerThread(iw, inReader, dim, vectorEncoding, fieldType, numDocsIndexed, numDocs);
                            threads.add(t);
                            t.setDaemon(true);
                            futures.add(exec.submit(t));
                        }
                        for (Future<?> future : futures) {
                            future.get();
                        }
                        result.docAddTimeMS = TimeUnit.NANOSECONDS.toMillis(
                            threads.stream().mapToLong(x -> x.docAddTime).sum() / numIndexThreads
                        );
                    }
                }
            }
            logger.info("KnnIndexer: indexed {} documents of desired {} numDocs", numDocsIndexed, numDocs);
            logger.debug("all indexing threads finished, now IndexWriter.commit()");
            iw.commit();
            ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
            cms.sync();
        }

        long elapsed = System.nanoTime() - start;
        logger.debug("Indexing took {} ms for {} docs", TimeUnit.NANOSECONDS.toMillis(elapsed), numDocs);
        result.indexTimeMS = TimeUnit.NANOSECONDS.toMillis(elapsed);

        // report numDocsIndexed here in case we have less than the total numDocs
        result.numDocs = numDocsIndexed.get();
    }

    void forceMerge(KnnIndexTester.Results results, int maxNumSegments) throws Exception {
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        iwc.setInfoStream(new PrintStreamInfoStream(System.out) {
            @Override
            public boolean isEnabled(String component) {
                return Objects.equals(component, "IVF");
            }
        });
        iwc.setCodec(codec);
        iwc.setUseCompoundFile(false);
        logger.info("KnnIndexer: forceMerge in {} into {} segments", indexPath, maxNumSegments);
        long startNS = System.nanoTime();
        try (IndexWriter iw = new IndexWriter(getDirectory(indexPath), iwc)) {
            iw.forceMerge(maxNumSegments);
        }
        long endNS = System.nanoTime();
        long elapsedNSec = (endNS - startNS);
        logger.info("forceMerge took {} ms", TimeUnit.NANOSECONDS.toMillis(elapsedNSec));
        results.forceMergeTimeMS = TimeUnit.NANOSECONDS.toMillis(elapsedNSec);
    }

    static Directory getDirectory(Path indexPath) throws IOException {
        Directory dir = FSDirectory.open(indexPath);
        if (dir instanceof MMapDirectory mmapDir) {
            mmapDir.setReadAdvice(getReadAdviceFunc()); // enable madvise
            return new FsDirectoryFactory.HybridDirectory(NativeFSLockFactory.INSTANCE, mmapDir, 64);
        }
        return dir;
    }

    private static BiFunction<String, IOContext, Optional<ReadAdvice>> getReadAdviceFunc() {
        return (name, context) -> {
            if (context.hints().contains(StandardIOBehaviorHint.INSTANCE) || name.endsWith(".cfs")) {
                return Optional.of(ReadAdvice.NORMAL);
            }
            return MMapDirectory.ADVISE_BY_CONTEXT.apply(name, context);
        };
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

        long readTime;
        long docAddTime;

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
                int id = numDocsIndexed.get();
                if (id == numDocsToIndex) {
                    break;
                } else if (numDocsIndexed.compareAndSet(id, id + 1) == false) {
                    continue;
                }

                var startRead = System.nanoTime();
                final IndexableField field;
                switch (vectorEncoding) {
                    case BYTE -> {
                        in.next(byteVectorBuffer);
                        field = new KnnByteVectorField(VECTOR_FIELD, byteVectorBuffer, fieldType);
                    }
                    case FLOAT32 -> {
                        in.next(floatVectorBuffer);
                        field = new KnnFloatVectorField(VECTOR_FIELD, floatVectorBuffer, fieldType);
                    }
                    default -> throw new UnsupportedOperationException();
                }
                long endRead = System.nanoTime();
                readTime += (endRead - startRead);

                Document doc = new Document();
                doc.add(field);

                if ((id + 1) % 25000 == 0) {
                    logger.debug("Done indexing " + (id + 1) + " documents.");
                }
                doc.add(new StoredField(ID_FIELD, id));
                iw.addDocument(doc);

                docAddTime += (System.nanoTime() - endRead);
            }
        }
    }

    static class VectorReader {
        final float[] target;
        final int offsetByteSize;
        final ByteBuffer bytes;
        final FileChannel input;
        long position;

        static VectorReader create(FileChannel input, int dim, VectorEncoding vectorEncoding, int offsetByteSize) throws IOException {
            // check if dim is set as preamble in the file:
            int bufferSize = dim * vectorEncoding.byteSize;
            if (input.size() % ((long) dim * vectorEncoding.byteSize + offsetByteSize) != 0) {
                throw new IllegalArgumentException(
                    "vectors file \"" + input + "\" does not contain a whole number of vectors?  size=" + input.size()
                );
            }
            return new VectorReader(input, dim, bufferSize, offsetByteSize);
        }

        VectorReader(FileChannel input, int dim, int bufferSize, int offsetByteSize) throws IOException {
            this.offsetByteSize = offsetByteSize;
            this.bytes = ByteBuffer.wrap(new byte[bufferSize]).order(ByteOrder.LITTLE_ENDIAN);
            this.input = input;
            this.target = new float[dim];
            reset();
        }

        void reset() throws IOException {
            position = offsetByteSize;
            input.position(position);
        }

        private void readNext() throws IOException {
            int bytesRead = Channels.readFromFileChannel(this.input, position, bytes);
            if (bytesRead < bytes.capacity()) {
                position = offsetByteSize;
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
            position += bytesRead + offsetByteSize;
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
