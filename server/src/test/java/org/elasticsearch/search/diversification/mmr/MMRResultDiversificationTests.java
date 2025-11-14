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
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MMRResultDiversificationTests extends ESTestCase {

    public void testMMRDiversification() throws IOException {
        for (int x = 0; x < 10; x++) {
            List<Integer> expectedDocIds = new ArrayList<>();
            MMRResultDiversificationContext diversificationContext = getRandomContext(expectedDocIds);

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
            assertNotSame(docs, diversifiedTopDocs);

            assertEquals(expectedDocIds.size(), diversifiedTopDocs.length);
            for (int i = 0; i < expectedDocIds.size(); i++) {
                assertEquals((int) expectedDocIds.get(i), diversifiedTopDocs[i].doc);
            }
        }
    }

    private MMRResultDiversificationContext getRandomContext(List<Integer> expectedDocIds) {
        if (randomBoolean()) {
            return getRandomFloatContext(expectedDocIds);
        }
        return getRandomByteContext(expectedDocIds);
    }

    private MMRResultDiversificationContext getRandomFloatContext(List<Integer> expectedDocIds) {
        final MapperBuilderContext context = MapperBuilderContext.root(false, false);

        DenseVectorFieldMapper mapper = new DenseVectorFieldMapper.Builder("dense_vector_field", IndexVersion.current(), false, List.of())
            .elementType(DenseVectorFieldMapper.ElementType.FLOAT)
            .dimensions(4)
            .build(context);

        DenseVectorFieldMapper.Builder builder = (DenseVectorFieldMapper.Builder) mapper.getMergeBuilder();
        builder.elementType(DenseVectorFieldMapper.ElementType.FLOAT);

        var queryVectorData = new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f });
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.3f, 3, queryVectorData);
        diversificationContext.setFieldVectors(
            Map.of(
                1,
                new VectorData(new float[] { 0.4f, 0.2f, 0.4f, 0.4f }),
                2,
                new VectorData(new float[] { 0.4f, 0.2f, 0.3f, 0.3f }),
                3,
                new VectorData(new float[] { 0.4f, 0.1f, 0.3f, 0.3f }),
                4,
                new VectorData(new float[] { 0.1f, 0.9f, 0.5f, 0.9f }),
                5,
                new VectorData(new float[] { 0.1f, 0.9f, 0.5f, 0.9f }),
                6,
                new VectorData(new float[] { 0.05f, 0.05f, 0.05f, 0.05f })
            )
        );

        expectedDocIds.addAll(List.of(1, 3, 6));

        return diversificationContext;
    }

    private MMRResultDiversificationContext getRandomByteContext(List<Integer> expectedDocIds) {
        final MapperBuilderContext context = MapperBuilderContext.root(false, false);

        DenseVectorFieldMapper mapper = new DenseVectorFieldMapper.Builder("dense_vector_field", IndexVersion.current(), false, List.of())
            .elementType(DenseVectorFieldMapper.ElementType.BYTE)
            .dimensions(4)
            .build(context);

        DenseVectorFieldMapper.Builder builder = (DenseVectorFieldMapper.Builder) mapper.getMergeBuilder();
        builder.elementType(DenseVectorFieldMapper.ElementType.BYTE);

        var queryVectorData = new VectorData(new byte[] { 0x50, 0x20, 0x40, 0x40 });
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.3f, 3, queryVectorData);
        diversificationContext.setFieldVectors(
            Map.of(
                1,
                new VectorData(new byte[] { 0x40, 0x20, 0x40, 0x40 }),
                2,
                new VectorData(new byte[] { 0x40, 0x20, 0x30, 0x30 }),
                3,
                new VectorData(new byte[] { 0x40, 0x10, 0x30, 0x30 }),
                4,
                new VectorData(new byte[] { 0x10, (byte) 0x90, 0x50, (byte) 0x90 }),
                5,
                new VectorData(new byte[] { 0x10, (byte) 0x90, 0x50, (byte) 0x90 }),
                6,
                new VectorData(new byte[] { 0x50, 0x50, 0x50, 0x50 })
            )
        );

        expectedDocIds.addAll(List.of(1, 3, 6));

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

        var queryVectorData = new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f });
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.6f, 10, queryVectorData);
        RankDoc[] emptyDocs = new RankDoc[0];

        MMRResultDiversification resultDiversification = new MMRResultDiversification(diversificationContext);

        assertSame(emptyDocs, resultDiversification.diversify(emptyDocs));
        assertNull(resultDiversification.diversify(null));
    }
}
