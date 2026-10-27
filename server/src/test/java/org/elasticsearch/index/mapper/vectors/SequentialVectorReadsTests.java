/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoReuseHint;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.DenseVectorBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DenseVectorBlockLoaderProcessor;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.search.stats.ShardFieldUsageTracker;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.index.store.StoreMetricsIndexInput;
import org.elasticsearch.script.field.vectors.KnnDenseVectorDocValuesField;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomFloatVector;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/** Brute-force reads of raw vectors kept only to rescore read them in order, on a shard's directory and readers. */
public class SequentialVectorReadsTests extends MapperServiceTestCase {

    private static final int DIMS = 64;
    private static final int NUM_DOCS = 256;

    private final List<Open> opens = new CopyOnWriteArrayList<>();
    private final List<float[]> vectors = new ArrayList<>();
    private MapperService mapperService;
    private DenseVectorFieldType fieldType;

    @Before
    public void assumeAdvice() {
        assumeTrue("reads open the raw vectors again only when their mapping is advised", MMapDirectory.supportsMadvise());
    }

    public void testScriptsReadInOrder() throws IOException {
        try (Directory dir = index(rescoreOnlyType()); DirectoryReader reader = open(dir)) {
            LeafReaderContext leaf = onlyLeaf(reader);
            var fieldData = fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "test")).build(null, null).load(leaf);
            opens.clear();
            var field = (KnnDenseVectorDocValuesField) fieldData.getScriptFieldFactory("field");
            assertThat(opens.toString(), sequentialOpens(IOContext.Context.DEFAULT), hasSize(1));
            for (int doc = 0; doc < NUM_DOCS; doc++) {
                field.setNextDocId(doc);
                assertArrayEquals("doc " + doc, vectors.get(doc), field.get().getVector(), tolerance());
            }
        }
    }

    public void testBlockLoadsReadInOrder() throws IOException {
        try (Directory dir = index(rescoreOnlyType()); DirectoryReader reader = open(dir)) {
            LeafReaderContext leaf = onlyLeaf(reader);
            var loader = new DenseVectorBlockLoader<>(
                "field",
                DIMS,
                fieldType,
                new DenseVectorBlockLoaderProcessor.DenseVectorLoaderProcessor()
            );
            opens.clear();
            BlockLoader.ColumnAtATimeReader columns = loader.reader(NoopCircuitBreaker.INSTANCE, leaf);
            assertThat(opens.toString(), sequentialOpens(IOContext.Context.DEFAULT), hasSize(1));
            TestBlock block = (TestBlock) columns.read(TestBlock.factory(), TestBlock.docs(leaf), 0, false);
            assertThat(block.size(), equalTo(NUM_DOCS));
            for (int doc = 0; doc < NUM_DOCS; doc++) {
                List<?> values = (List<?>) block.get(doc);
                float[] loaded = new float[values.size()];
                for (int i = 0; i < loaded.length; i++) {
                    loaded[i] = (Float) values.get(i);
                }
                assertArrayEquals("doc " + doc, vectors.get(doc), loaded, tolerance());
            }
        }
    }

    /** Only a query scoring every vector of the leaf reads them in order. */
    public void testOnlyTheBulkScorerReadsInOrder() throws IOException {
        try (Directory dir = index(rescoreOnlyType()); DirectoryReader reader = open(dir)) {
            IndexSearcher searcher = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                null,
                IndexSearcher.getDefaultQueryCachingPolicy(),
                true
            );
            VectorSimilarityFunction similarity = onlyLeaf(reader).reader()
                .getFieldInfos()
                .fieldInfo("field")
                .getVectorSimilarityFunction();
            DenseVectorQuery query = DenseVectorQuery.Floats.rawScored(randomFloatVector(DIMS), "field", similarity, false);
            Query tagged = new TermQuery(new Term("tag", "a"));

            opens.clear();
            Query conjunction = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                .add(tagged, BooleanClause.Occur.FILTER)
                .build();
            TopDocs conjunctionHits = searcher.search(conjunction, 10);
            assertThat("a conjunction picks the vectors it scores: " + opens, sequentialOpens(IOContext.Context.DEFAULT), hasSize(0));

            TopDocs scanHits = searcher.search(randomBoolean() ? query : query.filteredBy(tagged), 10);
            assertThat(opens.toString(), sequentialOpens(IOContext.Context.DEFAULT), hasSize(1));
            assertThat(scanHits.scoreDocs.length, equalTo(conjunctionHits.scoreDocs.length));
            for (int i = 0; i < scanHits.scoreDocs.length; i++) {
                assertThat(scanHits.scoreDocs[i].doc, equalTo(conjunctionHits.scoreDocs[i].doc));
                assertEquals(conjunctionHits.scoreDocs[i].score, scanHits.scoreDocs[i].score, tolerance());
            }
        }
    }

    /** Other reads, and brute-force reads of vectors searches keep resident, open nothing. */
    public void testOtherReadsOpenNothing() throws IOException {
        boolean rescoreOnly = randomBoolean();
        try (Directory dir = index(rescoreOnly ? rescoreOnlyType() : randomFrom("hnsw", "flat")); DirectoryReader reader = open(dir)) {
            LeafReader leaf = onlyLeaf(reader).reader();
            opens.clear();
            assertNotNull(leaf.getFloatVectorValues("field"));
            assertNotNull(leaf.getFloatVectorValues("field", DataAccessHint.RANDOM));
            if (rescoreOnly == false) {
                assertNotNull(leaf.getFloatVectorValues("field", DataAccessHint.SEQUENTIAL));
            }
            assertThat(opens.toString(), rawVectorOpens(), hasSize(0));
        }
    }

    /** Brute-force reads and merges share one input, opened by whichever comes first. */
    public void testBruteForceReadsAndMergesShareOneInput() throws IOException {
        boolean mergeFirst = randomBoolean();
        try (Directory dir = directory(rescoreOnlyType())) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig())) {
                indexSegment(writer, NUM_DOCS / 2);
                indexSegment(writer, NUM_DOCS / 2);
                try (
                    DirectoryReader reader = ElasticsearchDirectoryReader.wrap(
                        DirectoryReader.open(writer),
                        new ShardId("index", "_na_", 0)
                    )
                ) {
                    assertThat(reader.leaves(), hasSize(2));
                    opens.clear();
                    if (mergeFirst) {
                        writer.forceMerge(1);
                    }
                    for (LeafReaderContext leaf : reader.leaves()) {
                        assertNotNull(leaf.reader().getFloatVectorValues("field", DataAccessHint.SEQUENTIAL));
                    }
                    if (mergeFirst == false) {
                        writer.forceMerge(1);
                    }
                    List<Open> sequential = opens.stream()
                        .filter(Open::isRawVectors)
                        .filter(o -> o.context().hints().contains(DataAccessHint.SEQUENTIAL))
                        .filter(o -> o.name().startsWith("_0") || o.name().startsWith("_1"))
                        .toList();
                    assertThat("one input per source segment: " + opens, sequential, hasSize(2));
                    IOContext.Context first = mergeFirst ? IOContext.Context.MERGE : IOContext.Context.DEFAULT;
                    for (Open open : sequential) {
                        assertThat(open.toString(), open.context().context(), equalTo(first));
                        assertTrue(open.toString(), open.context().hints().contains(NoReuseHint.INSTANCE));
                    }
                }
            }
        }
    }

    /** The raw vectors of a field the format keeps only to rescore. */
    private static String rescoreOnlyType() {
        return randomFrom("int8_hnsw", "int4_hnsw", "int8_flat", "int4_flat", "bbq_hnsw", "bbq_flat");
    }

    /** One segment of a field of the given index type. */
    private Directory index(String indexType) throws IOException {
        Directory dir = directory(indexType);
        try (IndexWriter writer = new IndexWriter(dir, writerConfig())) {
            indexSegment(writer, NUM_DOCS);
        }
        return dir;
    }

    private void indexSegment(IndexWriter writer, int docs) throws IOException {
        for (int doc = 0; doc < docs; doc++) {
            float[] vector = randomFloatVector(DIMS);
            vectors.add(vector);
            writer.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", vector).field("tag", "a"))).rootDoc());
        }
        writer.commit();
    }

    private IndexWriterConfig writerConfig() {
        return new IndexWriterConfig().setCodec(new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null).codec("default"))
            .setUseCompoundFile(false);
    }

    /** A directory set up as a shard's. */
    private Directory directory(String indexType) throws IOException {
        String similarity = randomFrom("l2_norm", "cosine");
        mapperService = createMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "dense_vector");
            b.field("dims", DIMS);
            b.field("element_type", randomFrom("float", "bfloat16"));
            b.field("index", true);
            b.field("similarity", similarity);
            b.startObject("index_options").field("type", indexType).endObject();
            b.endObject();
            b.startObject("tag").field("type", "keyword").endObject();
        }));
        fieldType = (DenseVectorFieldType) mapperService.fieldType("field");
        MMapDirectory mmap = new MMapDirectory(createTempDir());
        mmap.setReadAdvice(FsDirectoryFactory.getReadAdviceFunc());
        MapperService mappings = mapperService;
        Directory hybrid = new FsDirectoryFactory.HybridDirectory(NativeFSLockFactory.INSTANCE, mmap, 64, mappings::mappingLookup);
        return new FilterDirectory(new RecordingDirectory(hybrid, opens)) {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                return StoreMetricsIndexInput.create(name, super.openInput(name, context), StoreMetrics.NOOP_HOLDER.singleThreaded());
            }
        };
    }

    /** A reader wrapped as searches wrap it. */
    private static DirectoryReader open(Directory dir) throws IOException {
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(dir), new ShardId("index", "_na_", 0));
        return new FieldUsageTrackingDirectoryReader(reader, new ShardFieldUsageTracker().createSession());
    }

    /** The sequential opens of the raw vectors in a context of the given type. */
    private List<Open> sequentialOpens(IOContext.Context type) {
        return rawVectorOpens().stream()
            .filter(o -> o.context().context() == type)
            .filter(o -> o.context().hints().contains(DataAccessHint.SEQUENTIAL))
            .filter(o -> o.context().hints().contains(NoReuseHint.INSTANCE))
            .toList();
    }

    private List<Open> rawVectorOpens() {
        return opens.stream().filter(Open::isRawVectors).toList();
    }

    private static LeafReaderContext onlyLeaf(DirectoryReader reader) {
        assertThat(reader.leaves(), hasSize(1));
        return reader.leaves().get(0);
    }

    /** bfloat16 keeps 8 bits of mantissa. */
    private float tolerance() {
        return fieldType.getElementType() == DenseVectorFieldMapper.ElementType.BFLOAT16 ? 1e-2f : 1e-5f;
    }

    /** One {@link Directory#openInput} call. */
    record Open(String name, IOContext context) {
        boolean isRawVectors() {
            return name.endsWith(".vec");
        }
    }

    /** Records every open; inputs are returned as they are. */
    private static class RecordingDirectory extends FilterDirectory {
        private final List<Open> opens;

        RecordingDirectory(Directory in, List<Open> opens) {
            super(in);
            this.opens = opens;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            opens.add(new Open(name, context));
            return super.openInput(name, context);
        }
    }
}
