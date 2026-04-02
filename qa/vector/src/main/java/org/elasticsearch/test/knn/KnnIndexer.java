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
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.StandardIOBehaviorHint;
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
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

public class KnnIndexer {
    public static final String ID_FIELD = "id";
    public static final String VECTOR_FIELD = "vector";
    public static final String PARTITION_ID_FIELD = "partition_id";

    private final List<Path> docsPath;
    private final Path indexPath;
    private final VectorEncoding vectorEncoding;
    private int dim;
    private final VectorSimilarityFunction similarityFunction;
    private final boolean normalizeVectors;
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
        boolean normalizeVectors,
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
        this.normalizeVectors = normalizeVectors;
        this.numDocs = numDocs;
        this.mergePolicy = mergePolicy;
        this.writerBufferSizeInMb = writerBufferSizeInMb;
        this.writerMaxBufferedDocs = writerMaxBufferedDocs;
    }

    void createIndex(KnnIndexTester.Results result) throws IOException, InterruptedException, ExecutionException {
        try (Directory dir = getDirectory(indexPath)) {
            createIndex(result, dir);
        }
    }

    void createIndex(KnnIndexTester.Results result, Directory dir) throws IOException, InterruptedException, ExecutionException {
        try (
            IndexVectorReader.MultiFileVectorReader reader = IndexVectorReader.MultiFileVectorReader.create(
                docsPath,
                dim,
                vectorEncoding,
                numDocs
            )
        ) {
            this.dim = reader.dim();
            createIndex(result, dir, reader, new DefaultDocumentFactory(), reader.totalDocs(), null);
        }
    }

    /**
     * Core indexing method that uses the provided vector reader and document factory to build the index.
     */
    void createIndex(
        KnnIndexTester.Results result,
        Directory dir,
        IndexVectorReader vectorReader,
        DocumentFactory documentFactory,
        int totalDocs,
        Sort indexSort
    ) throws IOException, InterruptedException, ExecutionException {
        if (dim <= 0) {
            throw new IllegalArgumentException("dimensions must be specified for generated data");
        }
        FieldType fieldType = switch (vectorEncoding) {
            case BYTE -> KnnByteVectorField.createFieldType(dim, similarityFunction);
            case FLOAT32 -> KnnFloatVectorField.createFieldType(dim, similarityFunction);
        };
        logger.debug(
            "KnnIndexer: using codec={}, vectorEncoding={}, dim={}, similarityFunction={}, normalizeVectors={}",
            codec.getName(),
            vectorEncoding,
            dim,
            similarityFunction,
            normalizeVectors
        );

        if (Files.exists(indexPath)) {
            logger.debug("KnnIndexer: existing index at {}", indexPath);
        } else {
            Files.createDirectories(indexPath);
        }

        long start = System.nanoTime();
        AtomicInteger numDocsIndexed = new AtomicInteger();
        IndexWriterConfig iwc = createIndexWriterConfig(indexSort);
        try (IndexWriter iw = new IndexWriter(dir, iwc)) {
            try (ExecutorService exec = Executors.newFixedThreadPool(numIndexThreads, r -> new Thread(r, "KnnIndexer-Thread"))) {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < numIndexThreads; i++) {
                    futures.add(
                        exec.submit(
                            new IndexerThread(
                                iw,
                                vectorReader,
                                vectorEncoding,
                                fieldType,
                                documentFactory,
                                normalizeVectors,
                                numDocsIndexed,
                                totalDocs
                            )
                        )
                    );
                }
                for (Future<?> future : futures) {
                    future.get();
                }
            }
            logger.info("KnnIndexer: indexed {} documents", totalDocs);
            iw.commit();
            ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
            cms.sync();
        }

        long elapsed = System.nanoTime() - start;
        logger.debug("Indexing took {} ms for {} docs", TimeUnit.NANOSECONDS.toMillis(elapsed), totalDocs);
        result.indexTimeMS = TimeUnit.NANOSECONDS.toMillis(elapsed);
        result.numDocs = totalDocs;
    }

    private IndexWriterConfig createIndexWriterConfig(Sort indexSort) {
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCodec(codec);
        iwc.setMaxBufferedDocs(writerMaxBufferedDocs);
        iwc.setRAMBufferSizeMB(writerBufferSizeInMb);
        iwc.setUseCompoundFile(false);
        if (mergePolicy != null) {
            iwc.setMergePolicy(mergePolicy);
        }
        iwc.setMaxFullFlushMergeWaitMillis(0);
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }
        iwc.setInfoStream(new PrintStreamInfoStream(System.out) {
            @Override
            public boolean isEnabled(String component) {
                return Objects.equals(component, "IVF");
            }
        });
        return iwc;
    }

    void forceMerge(KnnIndexTester.Results results, int maxNumSegments) throws Exception {
        try (Directory dir = getDirectory(indexPath)) {
            forceMerge(results, maxNumSegments, dir);
        }
    }

    void forceMerge(KnnIndexTester.Results results, int maxNumSegments, Directory dir) throws Exception {
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
        try (IndexWriter iw = new IndexWriter(dir, iwc)) {
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

    /**
     * Opens a frozen (searchable snapshot) directory for the given index path.
     */
    static Directory openFrozenDirectory(Path indexPath) throws IOException {
        Path workPath = indexPath.resolveSibling(indexPath.getFileName() + ".snap_work");
        Files.createDirectories(workPath);
        logger.info("Opening frozen snapshot directory for index at {} with work path {}", indexPath, workPath);
        return openSearchableSnapshotDirectory(indexPath, workPath);
    }

    /**
     * Creates a directory backed by searchable snapshot infrastructure, wrapping an existing
     * Lucene index on disk. Loaded via reflection because the factory resides in the
     * searchable-snapshots test artifact (unnamed module) which cannot be directly referenced
     * from this named module ({@code org.elasticsearch.test.knn}).
     */
    private static Directory openSearchableSnapshotDirectory(Path indexPath, Path workPath) throws IOException {
        try {
            Class<?> factoryClass = Class.forName("org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory");
            var method = factoryClass.getMethod("newDirectoryFromIndex", Path.class, Path.class);
            return (Directory) method.invoke(null, indexPath, workPath);
        } catch (Exception e) {
            throw new IOException(
                "Failed to create searchable snapshot directory. Ensure the searchable-snapshots test artifact is on the classpath.",
                e
            );
        }
    }

    private static BiFunction<String, IOContext, Optional<ReadAdvice>> getReadAdviceFunc() {
        return (name, context) -> {
            if (context.hints().contains(StandardIOBehaviorHint.INSTANCE) || name.endsWith(".cfs")) {
                return Optional.of(ReadAdvice.NORMAL);
            }
            return MMapDirectory.ADVISE_BY_CONTEXT.apply(name, context);
        };
    }

    /**
     * Creates a document from a vector field and document ordinal.
     */
    public interface DocumentFactory {
        Document createDocument(IndexableField vectorField, int docOrd);
    }

    /**
     * A {@link DocumentFactory} that creates documents with just a vector and a stored ID.
     */
    public static class DefaultDocumentFactory implements DocumentFactory {
        @Override
        public Document createDocument(IndexableField vectorField, int docOrd) {
            Document doc = new Document();
            doc.add(vectorField);
            doc.add(new StoredField(ID_FIELD, docOrd));
            return doc;
        }
    }

    /**
     * A {@link DocumentFactory} that creates documents with a vector, stored ID, and partition fields.
     */
    public static class PartitionDocumentFactory implements DocumentFactory {
        private final String[] docPartitionIds;
        private final int[] docOrdinals;

        public PartitionDocumentFactory(String[] docPartitionIds, int[] docOrdinals) {
            this.docPartitionIds = docPartitionIds;
            this.docOrdinals = docOrdinals;
        }

        @Override
        public Document createDocument(IndexableField vectorField, int docOrd) {
            Document doc = new Document();
            doc.add(vectorField);
            doc.add(new StoredField(ID_FIELD, docOrdinals[docOrd]));
            doc.add(SortedDocValuesField.indexedField(PARTITION_ID_FIELD, new BytesRef(docPartitionIds[docOrd])));
            return doc;
        }
    }

    static class IndexerThread implements Runnable {
        private final IndexWriter iw;
        private final IndexVectorReader vectorReader;
        private final VectorEncoding vectorEncoding;
        private final FieldType fieldType;
        private final DocumentFactory documentFactory;
        private final AtomicInteger numDocsIndexed;
        private final int numDocsToIndex;
        private final boolean normalizeVectors;

        IndexerThread(
            IndexWriter iw,
            IndexVectorReader vectorReader,
            VectorEncoding vectorEncoding,
            FieldType fieldType,
            DocumentFactory documentFactory,
            boolean normalizeVectors,
            AtomicInteger numDocsIndexed,
            int numDocsToIndex
        ) {
            this.iw = iw;
            this.vectorReader = vectorReader;
            this.vectorEncoding = vectorEncoding;
            this.fieldType = fieldType;
            this.documentFactory = documentFactory;
            this.normalizeVectors = normalizeVectors;
            this.numDocsIndexed = numDocsIndexed;
            this.numDocsToIndex = numDocsToIndex;
        }

        @Override
        public void run() {
            try {
                int idx;
                while ((idx = numDocsIndexed.getAndIncrement()) < numDocsToIndex) {

                    final IndexableField field;
                    switch (vectorEncoding) {
                        case BYTE -> {
                            byte[] vector = vectorReader.nextByteVector(idx);
                            field = new KnnByteVectorField(VECTOR_FIELD, vector, fieldType);
                        }
                        case FLOAT32 -> {
                            float[] vector = vectorReader.nextFloatVector(idx);
                            if (normalizeVectors) {
                                VectorUtil.l2normalize(vector);
                            }
                            field = new KnnFloatVectorField(VECTOR_FIELD, vector, fieldType);
                        }
                        default -> throw new UnsupportedOperationException();
                    }

                    Document doc = documentFactory.createDocument(field, idx);
                    iw.addDocument(doc);

                    if ((idx + 1) % 25000 == 0) {
                        logger.debug("Done indexing {} documents.", idx + 1);
                    }
                }
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        }
    }
}
