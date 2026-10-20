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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.Open;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.RecordingDirectory;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.DenseVectorBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DenseVectorBlockLoaderProcessor;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.script.field.vectors.KnnDenseVectorDocValuesField;
import org.elasticsearch.search.vectors.DenseVectorQuery;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.index.codec.vectors.SequentialVectorValuesTests.sequentialRawVectors;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/** Brute-force readers read raw vectors kept only to rescore sequentially, and read the indexed vectors. */
public class SequentialVectorReadsTests extends MapperServiceTestCase {

    private static final int DIMS = 64;
    private static final int NUM_DOCS = 256;

    private final List<Open> opens = new CopyOnWriteArrayList<>();
    private final float[][] vectors = new float[NUM_DOCS][];
    private DenseVectorFieldType fieldType;

    public void testScriptsReadInDocOrder() throws IOException {
        try (Directory dir = index(); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = onlyLeaf(reader);
            var fieldData = fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "test")).build(null, null).load(leaf);
            opens.clear();
            var field = (KnnDenseVectorDocValuesField) fieldData.getScriptFieldFactory("field");
            assertThat(opens, hasItem(sequentialRawVectors()));
            for (int doc = 0; doc < NUM_DOCS; doc++) {
                field.setNextDocId(doc);
                assertArrayEquals("doc " + doc, vectors[doc], field.get().getVector(), 1e-5f);
            }
        }
    }

    public void testBlockLoadsReadInDocOrder() throws IOException {
        try (Directory dir = index(); DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReaderContext leaf = onlyLeaf(reader);
            var loader = new DenseVectorBlockLoader<>(
                "field",
                DIMS,
                fieldType,
                new DenseVectorBlockLoaderProcessor.DenseVectorLoaderProcessor()
            );
            opens.clear();
            BlockLoader.ColumnAtATimeReader columns = loader.reader(NoopCircuitBreaker.INSTANCE, leaf);
            assertThat(opens, hasItem(sequentialRawVectors()));
            TestBlock block = (TestBlock) columns.read(TestBlock.factory(), TestBlock.docs(leaf), 0, false);
            assertThat(block.size(), equalTo(NUM_DOCS));
            for (int doc = 0; doc < NUM_DOCS; doc++) {
                List<?> values = (List<?>) block.get(doc);
                float[] loaded = new float[values.size()];
                for (int i = 0; i < loaded.length; i++) {
                    loaded[i] = (Float) values.get(i);
                }
                assertArrayEquals("doc " + doc, vectors[doc], loaded, 1e-5f);
            }
        }
    }

    /** A query scoring every vector of the leaf, filtered or not, reads them in doc order; one that a conjunction advances does not. */
    public void testOnlyTheBulkScorerReadsInDocOrder() throws IOException {
        try (Directory dir = index(); DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            VectorSimilarityFunction similarity = onlyLeaf(reader).reader()
                .getFieldInfos()
                .fieldInfo("field")
                .getVectorSimilarityFunction();
            float[] target = randomVector();
            DenseVectorQuery query = DenseVectorQuery.Floats.rawScored(target, "field", similarity, false);
            Query tagged = new TermQuery(new Term("tag", "a"));

            opens.clear();
            Query conjunction = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                .add(tagged, BooleanClause.Occur.FILTER)
                .build();
            TopDocs conjunctionHits = searcher.search(conjunction, 10);
            assertThat("a conjunction picks the vectors it scores, " + opens, opens, not(hasItem(sequentialRawVectors())));

            TopDocs scanHits = searcher.search(randomBoolean() ? query : query.filteredBy(tagged), 10);
            assertThat(opens, hasItem(sequentialRawVectors()));
            assertThat(scanHits.scoreDocs.length, equalTo(conjunctionHits.scoreDocs.length));
            for (int i = 0; i < scanHits.scoreDocs.length; i++) {
                assertThat(scanHits.scoreDocs[i].doc, equalTo(conjunctionHits.scoreDocs[i].doc));
                assertEquals(conjunctionHits.scoreDocs[i].score, scanHits.scoreDocs[i].score, 1e-5f);
            }
        }
    }

    /** One segment of {@link #NUM_DOCS} documents of a field whose quantized format keeps its raw vectors only to rescore. */
    private Directory index() throws IOException {
        String indexType = randomFrom("int8_hnsw", "int4_hnsw", "int8_flat", "bbq_hnsw", "bbq_flat");
        String similarity = randomFrom("l2_norm", "cosine");
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "dense_vector");
            b.field("dims", DIMS);
            b.field("index", true);
            b.field("similarity", similarity);
            b.startObject("index_options").field("type", indexType).endObject();
            b.endObject();
            b.startObject("tag").field("type", "keyword").endObject();
        }));
        fieldType = (DenseVectorFieldType) mapperService.fieldType("field");
        Directory dir = new RecordingDirectory(newDirectory(), opens);
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(
            new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null).codec("default")
        ).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int doc = 0; doc < NUM_DOCS; doc++) {
                float[] vector = randomVector();
                vectors[doc] = vector;
                writer.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", vector).field("tag", "a"))).rootDoc());
            }
        }
        return dir;
    }

    private static LeafReaderContext onlyLeaf(DirectoryReader reader) {
        assertThat(reader.leaves(), hasSize(1));
        return reader.leaves().get(0);
    }

    private static float[] randomVector() {
        float[] vector = new float[DIMS];
        for (int i = 0; i < DIMS; i++) {
            vector[i] = randomFloat() * 2 - 1;
        }
        return vector;
    }
}
