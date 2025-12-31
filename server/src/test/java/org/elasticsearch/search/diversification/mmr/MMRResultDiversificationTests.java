/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification.mmr;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.diversification.DiversifyRetrieverBuilder;
import org.elasticsearch.search.diversification.FieldVectorSupplier;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class MMRResultDiversificationTests extends ESTestCase {

    public void testMMRDiversification() throws IOException {
        for (int x = 0; x < 10; x++) {
            List<Integer> expectedDocIds = new ArrayList<>();
            DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits = getTestSearchHits();
            MMRResultDiversificationContext diversificationContext = getRandomContext(expectedDocIds, searchHits);

            RankDoc[] docs = new RankDoc[] {
                new RankDoc(1, 2.0f, 1),
                new RankDoc(2, 1.8f, 1),
                new RankDoc(3, 1.8f, 1),
                new RankDoc(4, 1.0f, 1),
                new RankDoc(5, 0.8f, 1),
                new RankDoc(6, 0.8f, 1) };

            int rankIndex = 1;
            for (RankDoc doc : docs) {
                doc.rank = rankIndex++;
            }

            MMRResultDiversification resultDiversification = new MMRResultDiversification(diversificationContext);
            RankDoc[] diversifiedTopDocs = resultDiversification.diversify(docs);

            // need to clean for GC
            cleanSearchHits(searchHits);
            searchHits = null;

            assertNotSame(docs, diversifiedTopDocs);

            assertEquals(expectedDocIds.size(), diversifiedTopDocs.length);
            for (int i = 0; i < expectedDocIds.size(); i++) {
                assertEquals((int) expectedDocIds.get(i), diversifiedTopDocs[i].doc);
            }
        }
    }

    private DiversifyRetrieverBuilder.RankDocWithSearchHit[] getTestSearchHits() {
        return new DiversifyRetrieverBuilder.RankDocWithSearchHit[] {
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(1, 1.0f, 1, new SearchHit(1)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(2, 1.0f, 1, new SearchHit(2)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(3, 1.0f, 1, new SearchHit(3)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(4, 1.0f, 1, new SearchHit(4)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(5, 1.0f, 1, new SearchHit(5)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(6, 1.0f, 1, new SearchHit(6)) };
    }

    private void cleanSearchHits(DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits) {
        for (int i = 0; i < searchHits.length; i++) {
            searchHits[i].hit().decRef();
        }
    }

    private MMRResultDiversificationContext getRandomContext(
        List<Integer> expectedDocIds,
        DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits
    ) {
        if (randomBoolean()) {
            return getRandomFloatContext(expectedDocIds, searchHits);
        }
        return getRandomByteContext(expectedDocIds, searchHits);
    }

    private MMRResultDiversificationContext getRandomFloatContext(
        List<Integer> expectedDocIds,
        DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits
    ) {
        final MapperBuilderContext context = MapperBuilderContext.root(false, false);

        DenseVectorFieldMapper mapper = new DenseVectorFieldMapper.Builder("dense_vector_field", IndexVersion.current(), false, List.of())
            .elementType(DenseVectorFieldMapper.ElementType.FLOAT)
            .dimensions(4)
            .build(context);

        DenseVectorFieldMapper.Builder builder = (DenseVectorFieldMapper.Builder) mapper.getMergeBuilder();
        builder.elementType(DenseVectorFieldMapper.ElementType.FLOAT);

        Supplier<VectorData> queryVectorData = () -> new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f });
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.3f, 3, queryVectorData);

        diversificationContext.setFieldVectors(
            searchHits,
            new MockFieldVectorSuppler(
                Map.of(
                    1,
                    List.of(new VectorData(new float[] { 0.4f, 0.2f, 0.4f, 0.4f })),
                    2,
                    List.of(new VectorData(new float[] { 0.4f, 0.2f, 0.3f, 0.3f })),
                    3,
                    List.of(new VectorData(new float[] { 0.4f, 0.1f, 0.3f, 0.3f })),
                    4,
                    List.of(new VectorData(new float[] { 0.1f, 0.9f, 0.5f, 0.9f })),
                    5,
                    List.of(new VectorData(new float[] { 0.1f, 0.9f, 0.5f, 0.9f })),
                    6,
                    List.of(new VectorData(new float[] { 0.05f, 0.05f, 0.05f, 0.05f }))
                )
            ),
            100
        );

        expectedDocIds.addAll(List.of(3, 4, 6));

        return diversificationContext;
    }

    private MMRResultDiversificationContext getRandomByteContext(
        List<Integer> expectedDocIds,
        DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits
    ) {
        final MapperBuilderContext context = MapperBuilderContext.root(false, false);

        DenseVectorFieldMapper mapper = new DenseVectorFieldMapper.Builder("dense_vector_field", IndexVersion.current(), false, List.of())
            .elementType(DenseVectorFieldMapper.ElementType.BYTE)
            .dimensions(4)
            .build(context);

        DenseVectorFieldMapper.Builder builder = (DenseVectorFieldMapper.Builder) mapper.getMergeBuilder();
        builder.elementType(DenseVectorFieldMapper.ElementType.BYTE);

        Supplier<VectorData> queryVectorData = () -> new VectorData(new byte[] { 0x50, 0x20, 0x40, 0x40 });
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.3f, 3, queryVectorData);

        DiversifyRetrieverBuilder.RankDocWithSearchHit[] results = new DiversifyRetrieverBuilder.RankDocWithSearchHit[] {
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(1, 1.0f, 1, new SearchHit(1)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(2, 1.0f, 1, new SearchHit(2)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(3, 1.0f, 1, new SearchHit(3)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(4, 1.0f, 1, new SearchHit(4)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(5, 1.0f, 1, new SearchHit(5)),
            new DiversifyRetrieverBuilder.RankDocWithSearchHit(6, 1.0f, 1, new SearchHit(6)), };

        diversificationContext.setFieldVectors(
            results,
            new MockFieldVectorSuppler(
                Map.of(
                    1,
                    List.of(new VectorData(new byte[] { 0x40, 0x20, 0x40, 0x40 })),
                    2,
                    List.of(new VectorData(new byte[] { 0x40, 0x20, 0x30, 0x30 })),
                    3,
                    List.of(new VectorData(new byte[] { 0x40, 0x10, 0x30, 0x30 })),
                    4,
                    List.of(new VectorData(new byte[] { 0x10, (byte) 0x90, 0x50, (byte) 0x90 })),
                    5,
                    List.of(new VectorData(new byte[] { 0x10, (byte) 0x90, 0x50, (byte) 0x90 })),
                    6,
                    List.of(new VectorData(new byte[] { 0x50, 0x50, 0x50, 0x50 }))
                )
            ),
            100
        );

        expectedDocIds.addAll(List.of(3, 4, 6));

        return diversificationContext;
    }

    public void testMMRDiversificationIfNoSearchHits() throws IOException {
        final MapperBuilderContext context = MapperBuilderContext.root(false, false);

        DenseVectorFieldMapper mapper = new DenseVectorFieldMapper.Builder("dense_vector_field", IndexVersion.current(), false, List.of())
            .elementType(DenseVectorFieldMapper.ElementType.FLOAT)
            .dimensions(4)
            .build(context);

        // Change the element type to byte, which is incompatible with int8 HNSW index options
        DenseVectorFieldMapper.Builder builder = (DenseVectorFieldMapper.Builder) mapper.getMergeBuilder();
        builder.elementType(DenseVectorFieldMapper.ElementType.FLOAT);

        Supplier<VectorData> queryVectorData = () -> new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f });
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.6f, 10, queryVectorData);
        RankDoc[] emptyDocs = new RankDoc[0];

        MMRResultDiversification resultDiversification = new MMRResultDiversification(diversificationContext);

        assertSame(emptyDocs, resultDiversification.diversify(emptyDocs));
        assertNull(resultDiversification.diversify(null));
    }

    private class MockFieldVectorSuppler implements FieldVectorSupplier {
        private final Map<Integer, List<VectorData>> vectors;

        MockFieldVectorSuppler(Map<Integer, List<VectorData>> vectors) {
            this.vectors = vectors;
        }

        @Override
        public Map<Integer, List<VectorData>> getFieldVectors(DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits) {
            return vectors;
        }
    }
}
