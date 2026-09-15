/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es816.BinaryQuantizer;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A BBQ merge writes the merged raw vectors once and must not read them back to build the graph: the
 * writer produces the query-side records in the same pass that produces the index-side records, and the
 * merge scorer picks those up. These tests pin that, and that the hand-off file never outlives the merge.
 */
public class ES818MergeQueryDataTests extends LuceneTestCase {

    static {
        LogConfigurator.configureESLogging(); // native vector scoring requires logging to be initialized
    }

    private static final int DIMS = 64;
    private static final int DOCS_PER_SEGMENT = 200;

    /** Counts the bytes read from every file, including through clones and slices, and the files created. */
    private static class ReadCountingDirectory extends FilterDirectory {
        final Map<String, AtomicLong> bytesRead = new ConcurrentHashMap<>();
        final Map<String, AtomicLong> created = new ConcurrentHashMap<>();

        ReadCountingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new CountingIndexInput(name, super.openInput(name, context), bytesRead.computeIfAbsent(name, n -> new AtomicLong()));
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            created.computeIfAbsent(name, n -> new AtomicLong()).incrementAndGet();
            return super.createOutput(name, context);
        }

        long bytesRead(String name) {
            AtomicLong count = bytesRead.get(name);
            return count == null ? 0 : count.get();
        }

        /** The hand-off files the writer created, one per field of a merge that builds a graph. */
        List<String> handOffsCreated() {
            return created.keySet()
                .stream()
                .filter(n -> n.contains(ES818BinaryQuantizedVectorsWriter.MERGE_QUERIES_TEMP_SUFFIX))
                .sorted()
                .toList();
        }
    }

    private static class CountingIndexInput extends IndexInput {
        private final IndexInput in;
        private final AtomicLong counter;

        CountingIndexInput(String description, IndexInput in, AtomicLong counter) {
            super(description);
            this.in = in;
            this.counter = counter;
        }

        @Override
        public byte readByte() throws IOException {
            counter.incrementAndGet();
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            counter.addAndGet(len);
            in.readBytes(b, offset, len);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new CountingIndexInput(sliceDescription, in.slice(sliceDescription, offset, length), counter);
        }

        @Override
        public IndexInput clone() {
            return new CountingIndexInput(toString(), in.clone(), counter);
        }
    }

    private static KnnVectorsFormat bbqHnsw(int graphThreshold, DenseVectorFieldMapper.ElementType elementType) {
        return new ES93HnswBinaryQuantizedVectorsFormat(16, 100, elementType, false, 1, null, graphThreshold);
    }

    public void testMergeDoesNotReadMergedRawVectorsBack() throws IOException {
        // threshold 0: the merge always builds a graph, so the merge scorer is always requested
        runMerge(bbqHnsw(0, DenseVectorFieldMapper.ElementType.FLOAT), true, VectorSimilarityFunction.EUCLIDEAN);
    }

    public void testMergeDoesNotReadMergedRawVectorsBackCosine() throws IOException {
        // cosine normalizes before quantizing on both the writer's and the read-back path
        runMerge(bbqHnsw(0, DenseVectorFieldMapper.ElementType.FLOAT), true, VectorSimilarityFunction.COSINE);
    }

    public void testMergeDoesNotReadMergedRawVectorsBackBFloat16() throws IOException {
        runMerge(bbqHnsw(0, DenseVectorFieldMapper.ElementType.BFLOAT16), true, VectorSimilarityFunction.DOT_PRODUCT);
    }

    public void testQueryTempIsCleanedWhenNoGraphIsBuilt() throws IOException {
        // a threshold the merge does not reach: the writer mirrors the graph decision and writes no
        // hand-off file (asserted), and nothing may outlive the merge; the merge scorer is never requested
        runMerge(bbqHnsw(Integer.MAX_VALUE, DenseVectorFieldMapper.ElementType.FLOAT), false, VectorSimilarityFunction.EUCLIDEAN);
    }

    public void testDefaultThresholdWritesNoHandOffBelowIt() throws IOException {
        // the production configuration: the mapper passes -1, which the format resolves to its default
        // (300). Two segments of 200 vectors stay below that threshold's expected search cost, so Lucene
        // builds no graph and the writer must produce no hand-off either: the mirror has to see the
        // resolved threshold, not the -1
        runMerge(bbqHnsw(-1, DenseVectorFieldMapper.ElementType.FLOAT), false, VectorSimilarityFunction.EUCLIDEAN);
    }

    /**
     * The writer mirrors the graph decision, so a leftover hand-off file needs the mirror to disagree with
     * Lucene; the reader the graph build opens on the segment being merged deletes one regardless.
     */
    public void testMergeContextReaderDeletesLeftoverTemp() throws IOException {
        Path path = createTempDir("bbqLeftoverTemp");
        IndexWriterConfig config = new IndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(bbqHnsw(0, DenseVectorFieldMapper.ElementType.FLOAT))
        );
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);
        try (Directory dir = new MMapDirectory(path); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < 2 * DOCS_PER_SEGMENT; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("v", randomVector(DIMS), VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
                if (i == DOCS_PER_SEGMENT - 1) {
                    writer.commit();
                }
            }
            writer.commit();
            writer.forceMerge(1);
            writer.commit();
            SegmentCommitInfo merged = SegmentInfos.readLatestCommit(dir).info(0);
            // the per-field format suffix, from the quantized data file's name
            String veb = merged.files().stream().filter(f -> f.endsWith(".veb")).findFirst().orElseThrow();
            String suffix = veb.substring(merged.info.name.length() + 1, veb.length() - ".veb".length());
            FieldInfos fieldInfos;
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                fieldInfos = ((SegmentReader) getOnlyLeafReader(reader)).getFieldInfos();
            }
            int fieldNumber = fieldInfos.fieldInfo("v").number;
            String leftover = ES818BinaryQuantizedVectorsWriter.mergeQueriesTempName(merged.info.name, suffix, fieldNumber);
            String someoneElses = ES818BinaryQuantizedVectorsWriter.mergeQueriesTempName("_zz", suffix, fieldNumber);
            for (String name : new String[] { leftover, someoneElses }) {
                try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
                    out.writeInt(42);
                }
            }
            // the flat reader is what owns the hand-off files; open it the two ways a merge and a search do
            ES93BinaryQuantizedVectorsFormat flat = new ES93BinaryQuantizedVectorsFormat(
                DenseVectorFieldMapper.ElementType.FLOAT,
                false,
                true,
                0
            );
            // a search-time (DEFAULT context) reader must leave both alone
            flat.fieldsReader(new SegmentReadState(dir, merged.info, fieldInfos, IOContext.DEFAULT, suffix)).close();
            assertTrue(Arrays.asList(dir.listAll()).containsAll(List.of(leftover, someoneElses)));
            // the reader a merge opens on the segment it is writing deletes its own fields' leftover, only that
            flat.fieldsReader(new SegmentReadState(dir, merged.info, fieldInfos, IOContext.merge(new MergeInfo(1, 1, false, -1)), suffix))
                .close();
            List<String> after = Arrays.asList(dir.listAll());
            assertFalse("the leftover hand-off file survived the merge-context reader's close", after.contains(leftover));
            assertTrue("another segment's file was deleted", after.contains(someoneElses));
            dir.deleteFile(someoneElses);
        }
    }

    /**
     * There is no read-back path: the merge scorer can only be built from the writer's records. A reader
     * asked for it without them fails loudly and names the missing file, and a file of that name that is
     * not this segment's (here: junk) is a corrupt-index error, never something to score a graph from.
     */
    public void testMergeScorerWithoutWriterRecordsFailsLoudly() throws IOException {
        Path path = createTempDir("bbqNoHandOff");
        IndexWriterConfig config = new IndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(bbqHnsw(0, DenseVectorFieldMapper.ElementType.FLOAT))
        );
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);
        try (Directory dir = new MMapDirectory(path); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < DOCS_PER_SEGMENT; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("v", randomVector(DIMS), VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
            }
            writer.commit(); // a flushed segment: written without a merge, so without a hand-off
            SegmentCommitInfo segment = SegmentInfos.readLatestCommit(dir).info(0);
            String veb = segment.files().stream().filter(f -> f.endsWith(".veb")).findFirst().orElseThrow();
            String suffix = veb.substring(segment.info.name.length() + 1, veb.length() - ".veb".length());
            FieldInfos fieldInfos;
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                fieldInfos = ((SegmentReader) getOnlyLeafReader(reader)).getFieldInfos();
            }
            FieldInfo field = fieldInfos.fieldInfo("v");
            String handOff = ES818BinaryQuantizedVectorsWriter.mergeQueriesTempName(segment.info.name, suffix, field.number);
            ES93BinaryQuantizedVectorsFormat flat = new ES93BinaryQuantizedVectorsFormat(
                DenseVectorFieldMapper.ElementType.FLOAT,
                false,
                true,
                0
            );
            IOContext merge = IOContext.merge(new MergeInfo(1, 1, false, -1));
            SegmentWriteState writeState = new SegmentWriteState(
                InfoStream.getDefault(),
                dir,
                segment.info,
                fieldInfos,
                null,
                merge,
                suffix
            );

            try (
                var reader = (ES818BinaryQuantizedVectorsReader) flat.fieldsReader(
                    new SegmentReadState(dir, segment.info, fieldInfos, merge, suffix)
                )
            ) {
                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> reader.getRandomVectorScorerSupplierForMerge(field, writeState)
                );
                assertTrue("the error must name the missing file: " + e.getMessage(), e.getMessage().contains(handOff));
            }

            try (IndexOutput out = dir.createOutput(handOff, IOContext.DEFAULT)) {
                out.writeInt(42);
            }
            try (
                var reader = (ES818BinaryQuantizedVectorsReader) flat.fieldsReader(
                    new SegmentReadState(dir, segment.info, fieldInfos, merge, suffix)
                )
            ) {
                expectThrows(CorruptIndexException.class, () -> reader.getRandomVectorScorerSupplierForMerge(field, writeState));
            }
            // the merge-context reader's close swept the junk file as a leftover of its own field
            assertFalse("the junk file outlived the merge-context reader", Arrays.asList(dir.listAll()).contains(handOff));
        }
    }

    /**
     * A merge that fails before the graph build never opens the reader that owns the hand-off files, and
     * Lucene deletes a failed merge's files only once the merge has registered them, which a merge that
     * fails this early never does. The writer's close must sweep what it wrote.
     */
    public void testPhaseOneAbortLeavesNoHandOff() throws IOException {
        Path path = createTempDir("bbqPhaseOneAbort");
        IndexWriterConfig config = new IndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(bbqHnsw(0, DenseVectorFieldMapper.ElementType.FLOAT))
        );
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);
        // merge on the calling thread, so the failure surfaces from forceMerge rather than from a merge thread
        config.setMergeScheduler(new SerialMergeScheduler());
        // the second field's hand-off cannot be created: phase 1 aborts after the first field wrote its own
        FilterDirectory dir = new FilterDirectory(new MMapDirectory(path)) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                if (name.contains("_" + ES818BinaryQuantizedVectorsWriter.MERGE_QUERIES_TEMP_SUFFIX)
                    && name.endsWith("bbqmq0.tmp") == false) {
                    throw new IOException("simulated failure creating " + name);
                }
                return super.createOutput(name, context);
            }
        };
        IndexWriter writer = new IndexWriter(dir, config);
        try {
            for (int i = 0; i < 2 * DOCS_PER_SEGMENT; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("v", randomVector(DIMS), VectorSimilarityFunction.EUCLIDEAN));
                doc.add(new KnnFloatVectorField("w", randomVector(DIMS), VectorSimilarityFunction.EUCLIDEAN));
                writer.addDocument(doc);
                if (i == DOCS_PER_SEGMENT - 1) {
                    writer.commit();
                }
            }
            writer.commit();
            Exception failure = expectThrows(Exception.class, () -> writer.forceMerge(1));
            // the merge failure surfaces wrapped (IndexWriter marks itself tragic); the cause chain carries it
            boolean simulated = false;
            for (Throwable t = failure; t != null && simulated == false; t = t.getCause()) {
                simulated = String.valueOf(t.getMessage()).contains("simulated failure");
            }
            assertTrue("unexpected merge failure: " + failure, simulated);
        } finally {
            try {
                writer.rollback();
            } catch (Exception e) {
                // the writer may already be unusable after the merge failure
            }
        }
        for (String file : dir.listAll()) {
            assertFalse("a hand-off file survived the aborted merge: " + file, file.endsWith(".tmp"));
        }
        dir.close();
    }

    public void testMergeQueriesTempName() {
        assertEquals("_5_bbqmq7.tmp", ES818BinaryQuantizedVectorsWriter.mergeQueriesTempName("_5", "", 7));
        assertEquals(
            "_5_ES93HnswBinaryQuantizedVectorsFormat_0_bbqmq70.tmp",
            ES818BinaryQuantizedVectorsWriter.mergeQueriesTempName("_5", "ES93HnswBinaryQuantizedVectorsFormat_0", 70)
        );
        // never a segment file: Lucene rejects .tmp names in a segment's file set
        assertTrue(ES818BinaryQuantizedVectorsWriter.mergeQueriesTempName("_5", "x", 1).endsWith(".tmp"));
    }

    /** The fused pass quantizes both widths from one centering pass; it must equal two independent quantizations. */
    public void testMultiBitQuantizationMatchesSingleBit() {
        for (int dims : new int[] { DIMS, 1024, 2048 }) {
            for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
                quantizationMatches(similarity, dims);
            }
        }
    }

    private static void quantizationMatches(VectorSimilarityFunction similarity, int dims) {
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarity);
        float[] centroid = randomVector(dims);
        for (int i = 0; i < 20; i++) {
            float[] vector = randomVector(dims);
            if (similarity == VectorSimilarityFunction.COSINE) {
                VectorUtil.l2normalize(vector);
                VectorUtil.l2normalize(centroid);
            }
            int[] expectedIndex = new int[dims];
            int[] expectedQuery = new int[dims];
            OptimizedScalarQuantizer.QuantizationResult expectedIndexResult = quantizer.scalarQuantize(
                vector,
                new float[dims],
                expectedIndex,
                ES818BinaryQuantizedVectorsWriter.INDEX_BITS,
                centroid
            );
            OptimizedScalarQuantizer.QuantizationResult expectedQueryResult = quantizer.scalarQuantize(
                vector,
                new float[dims],
                expectedQuery,
                BinaryQuantizer.B_QUERY,
                centroid
            );
            int[] actualIndex = new int[dims];
            int[] actualQuery = new int[dims];
            OptimizedScalarQuantizer.QuantizationResult[] actual = quantizer.multiScalarQuantize(
                vector,
                new float[dims],
                new int[][] { actualIndex, actualQuery },
                new byte[] { ES818BinaryQuantizedVectorsWriter.INDEX_BITS, BinaryQuantizer.B_QUERY },
                centroid
            );
            // the fused pass runs the same interval optimisation as the single-width call, but the
            // vectorised loss reduction is not bit-stable across JIT tiers (its lane sums may round
            // differently once compiled), which moves an optimised interval by an ulp and can flip a
            // quantised value sitting on a rounding boundary; the comparison allows exactly that much
            String where = similarity + " dims=" + dims + " vector#" + i;
            assertQuantizedClose(where + " index side", expectedIndex, actualIndex);
            assertQuantizedClose(where + " query side", expectedQuery, actualQuery);
            assertCorrectionsClose(where + " index-side corrections", dims, expectedIndexResult, actual[0]);
            assertCorrectionsClose(where + " query-side corrections", dims, expectedQueryResult, actual[1]);
        }
    }

    private static void assertQuantizedClose(String where, int[] expected, int[] actual) {
        assertEquals(where, expected.length, actual.length);
        int flipped = 0;
        for (int i = 0; i < expected.length; i++) {
            int diff = Math.abs(expected[i] - actual[i]);
            assertTrue(where + ": element " + i + " expected " + expected[i] + " but was " + actual[i], diff <= 1);
            flipped += diff;
        }
        assertTrue(where + ": " + flipped + " boundary flips in " + expected.length + " values", flipped <= 1 + expected.length / 100);
    }

    private static void assertCorrectionsClose(
        String where,
        int dims,
        OptimizedScalarQuantizer.QuantizationResult expected,
        OptimizedScalarQuantizer.QuantizationResult actual
    ) {
        assertClose(where + " lowerInterval", expected.lowerInterval(), actual.lowerInterval());
        assertClose(where + " upperInterval", expected.upperInterval(), actual.upperInterval());
        assertClose(where + " additionalCorrection", expected.additionalCorrection(), actual.additionalCorrection());
        // the component sum moves by one per boundary flip, no more than assertQuantizedClose allows
        assertTrue(
            where + " quantizedComponentSum expected " + expected.quantizedComponentSum() + " but was " + actual.quantizedComponentSum(),
            Math.abs(expected.quantizedComponentSum() - actual.quantizedComponentSum()) <= 1 + dims / 100
        );
    }

    private static void assertClose(String where, float expected, float actual) {
        assertEquals(where, expected, actual, 1e-5f * Math.max(1f, Math.abs(expected)));
    }

    /**
     * The query-side records the writer produces read back through the scorer's record reader exactly as
     * the quantizer produced them: the packed 4-bit values and the corrective terms, by random ordinal.
     */
    public void testWriterQueryRecordsRoundTripThroughTheScorerReader() throws IOException {
        int count = 50;
        float[] centroid = randomVector(DIMS);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(VectorSimilarityFunction.EUCLIDEAN);
        int discretized = org.elasticsearch.index.codec.vectors.BQVectorUtils.discretize(DIMS, 64);
        int[] indexQuantized = new int[DIMS];
        int[] queryQuantized = new int[DIMS];
        byte[] indexPacked = new byte[discretized / 8];
        byte[] queryPacked = new byte[(discretized / 8) * BinaryQuantizer.B_QUERY];
        byte[][] expectedPacked = new byte[count][];
        OptimizedScalarQuantizer.QuantizationResult[] expectedTerms = new OptimizedScalarQuantizer.QuantizationResult[count];
        try (Directory dir = new ByteBuffersDirectory()) {
            // the writer's pass, as writeBinarizedVectorAndMergeQueryData does it
            try (
                IndexOutput index = dir.createOutput("index", IOContext.DEFAULT);
                IndexOutput queries = dir.createOutput("queries", IOContext.DEFAULT)
            ) {
                for (int i = 0; i < count; i++) {
                    OptimizedScalarQuantizer.QuantizationResult[] results = quantizer.multiScalarQuantize(
                        randomVector(DIMS),
                        new float[DIMS],
                        new int[][] { indexQuantized, queryQuantized },
                        new byte[] { ES818BinaryQuantizedVectorsWriter.INDEX_BITS, BinaryQuantizer.B_QUERY },
                        centroid
                    );
                    ES818BinaryQuantizedVectorsWriter.writeIndexRecord(index, indexQuantized, indexPacked, results[0]);
                    ES818BinaryQuantizedVectorsWriter.writeQueryRecord(queries, queryQuantized, queryPacked, results[1]);
                    expectedPacked[i] = queryPacked.clone();
                    expectedTerms[i] = results[1];
                }
            }
            assertEquals(
                "one record per vector, of the size the reader expects",
                (long) count * ES818BinaryQuantizedVectorsReader.OffHeapBinarizedQueryVectorValues.recordSize(DIMS),
                dir.fileLength("queries")
            );
            try (IndexInput in = dir.openInput("queries", IOContext.DEFAULT)) {
                var records = new ES818BinaryQuantizedVectorsReader.OffHeapBinarizedQueryVectorValues(in, DIMS, count);
                for (int ord : new int[] { 0, count - 1, count / 2, 1, count / 2 }) {
                    assertArrayEquals("query bits of ordinal " + ord, expectedPacked[ord], records.vectorValue(ord));
                    assertEquals("corrective terms of ordinal " + ord, expectedTerms[ord], records.getCorrectiveTerms(ord));
                }
            }
        }
    }

    private void runMerge(KnnVectorsFormat format, boolean expectGraph, VectorSimilarityFunction similarity) throws IOException {
        float[][] vectors = new float[DOCS_PER_SEGMENT * 2][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = randomVector(DIMS);
        }
        Path path = createTempDir("bbqMergeQueryData");
        IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        // keep the raw vector file visible as its own file, not hidden inside a .cfs
        config.setUseCompoundFile(false);
        config.getMergePolicy().setNoCFSRatio(0.0);

        try (
            ReadCountingDirectory dir = new ReadCountingDirectory(new MMapDirectory(path));
            IndexWriter writer = new IndexWriter(dir, config)
        ) {
            for (int i = 0; i < vectors.length; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("v", vectors[i], similarity));
                // a second vector field of the same format: its own hand-off file, its own cleanup
                doc.add(new KnnFloatVectorField("w", vectors[vectors.length - 1 - i], similarity));
                writer.addDocument(doc);
                if (i == DOCS_PER_SEGMENT - 1) {
                    writer.commit();
                }
            }
            writer.commit();
            SegmentInfos before = SegmentInfos.readLatestCommit(dir);
            assertEquals(2, before.size());
            long sourceRawBytes = 0;
            for (SegmentCommitInfo sci : before) {
                for (String file : sci.files()) {
                    if (file.endsWith(".vec")) {
                        sourceRawBytes += dir.fileLength(file);
                    }
                }
            }
            assertTrue("expected raw vector files for the sources", sourceRawBytes >= 2L * vectors.length * DIMS * Float.BYTES / 2);

            // hold a reader open across the merge, as a serving node does
            try (DirectoryReader beforeMerge = DirectoryReader.open(writer)) {
                assertEquals(vectors.length, beforeMerge.numDocs());
                writer.forceMerge(1);
            }
            writer.commit();

            SegmentInfos after = SegmentInfos.readLatestCommit(dir);
            assertEquals(1, after.size());
            SegmentCommitInfo merged = after.info(0);
            String mergedRaw = null;
            for (String file : merged.files()) {
                assertFalse("a temp file was registered as a segment file: " + file, file.endsWith(".tmp"));
                if (file.endsWith(".vec")) {
                    mergedRaw = file;
                }
            }
            assertNotNull("merged segment has no raw vector file", mergedRaw);
            for (String file : dir.listAll()) {
                assertFalse("a temp file outlived the merge: " + file, file.endsWith(".tmp"));
            }
            // the writer produces one hand-off per field exactly when the merge builds a graph, and the
            // merge scorer then reads it; without a graph it produces none rather than one it deletes
            List<String> handOffs = dir.handOffsCreated();
            assertEquals("hand-off files created: " + handOffs, expectGraph ? 2 : 0, handOffs.size());
            for (String handOff : handOffs) {
                assertTrue("the merge scorer never read the hand-off " + handOff, dir.bytesRead(handOff) > 0);
            }

            long mergedRawBytes = 2L * vectors.length * DIMS * Float.BYTES; // two fields share the raw vector file
            long readBack = dir.bytesRead(mergedRaw);
            // the merge may touch the merged raw file's header, metadata and footer; it must not stream
            // the vectors back, which would read at least the whole file once
            assertTrue(
                "the merge read the merged raw vectors back: " + readBack + " of " + mergedRawBytes + " bytes",
                readBack < mergedRawBytes / 4
            );
            // positive control for the counter: the sources' raw vectors were streamed at least once
            long sourceReads = 0;
            for (SegmentCommitInfo sci : before) {
                for (String file : sci.files()) {
                    if (file.endsWith(".vec")) {
                        sourceReads += dir.bytesRead(file);
                    }
                }
            }
            assertTrue("expected the merge to read the source raw vectors, read " + sourceReads, sourceReads >= sourceRawBytes);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(vectors.length, reader.numDocs());
                float[] query = vectors[0].clone();
                if (similarity == VectorSimilarityFunction.COSINE) {
                    VectorUtil.l2normalize(query); // the field mapper normalizes cosine queries; the scorer asserts it
                }
                TopDocs topDocs = new IndexSearcher(reader).search(new KnnFloatVectorQuery("v", query, 5), 5);
                assertEquals(5, topDocs.scoreDocs.length);
                // the graph file exists either way; ask the reader whether a graph was actually built
                KnnVectorsReader vectorsReader = ((CodecReader) getOnlyLeafReader(reader)).getVectorReader();
                if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
                    vectorsReader = perField.getFieldReader("v");
                }
                HnswGraph graph = ((HnswGraphProvider) vectorsReader).getGraph("v");
                assertEquals("graph presence", expectGraph, graph != null && graph.size() > 0);
            }
        }
    }

    private static float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = random().nextFloat();
        }
        return vector;
    }
}
