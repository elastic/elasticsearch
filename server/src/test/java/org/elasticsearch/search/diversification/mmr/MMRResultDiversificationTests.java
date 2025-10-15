/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification.mmr;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MMRResultDiversificationTests extends ESTestCase {

    public void testMMRDiversification() throws IOException {
        final MapperBuilderContext context = MapperBuilderContext.root(false, false);

        DenseVectorFieldMapper mapper = new DenseVectorFieldMapper.Builder("dense_vector_field", IndexVersion.current(), false, List.of())
            .elementType(DenseVectorFieldMapper.ElementType.FLOAT)
            .dimensions(4)
            .build(context);

        DenseVectorFieldMapper.Builder builder = (DenseVectorFieldMapper.Builder) mapper.getMergeBuilder();
        builder.elementType(DenseVectorFieldMapper.ElementType.FLOAT);
        DenseVectorFieldMapper fieldMapper = builder.build(context);

        var queryVectorData = new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f });
        Map<Integer, VectorData> fieldVectors = Map.of(
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
        );
        var diversificationContext = new MMRResultDiversificationContext(
            "dense_vector_field",
            0.3f,
            3,
            fieldMapper,
            IndexVersion.current(),
            queryVectorData,
            fieldVectors
        );

        RankDoc[] docs = new RankDoc[] {
            new RankDoc(1, 2.0f, 1),
            new RankDoc(2, 1.8f, 1),
            new RankDoc(3, 1.8f, 1),
            new RankDoc(4, 1.0f, 1),
            new RankDoc(5, 0.8f, 1),
            new RankDoc(6, 0.8f, 1) };

        TotalHits totalHits = new TotalHits(6L, TotalHits.Relation.EQUAL_TO);

        MMRResultDiversification resultDiversification = new MMRResultDiversification();
        RankDoc[] diversifiedTopDocs = resultDiversification.diversify(docs, diversificationContext);
        assertNotSame(docs, diversifiedTopDocs);

        assertEquals(3, diversifiedTopDocs.length);
        assertEquals(1, diversifiedTopDocs[0].doc);
        assertEquals(3, diversifiedTopDocs[1].doc);
        assertEquals(6, diversifiedTopDocs[2].doc);
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
        DenseVectorFieldMapper fieldMapper = builder.build(context);

        var queryVectorData = new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f });
        var diversificationContext = new MMRResultDiversificationContext(
            "dense_vector_field",
            0.6f,
            10,
            fieldMapper,
            IndexVersion.current(),
            queryVectorData,
            new HashMap<>()
        );
        RankDoc[] emptyDocs = new RankDoc[0];

        MMRResultDiversification resultDiversification = new MMRResultDiversification();

        assertSame(emptyDocs, resultDiversification.diversify(emptyDocs, diversificationContext));
        assertNull(resultDiversification.diversify(null, diversificationContext));
    }
}
