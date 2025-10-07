/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification.mmr;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
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

        // Change the element type to byte, which is incompatible with int8 HNSW index options
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
            queryVectorData,
            fieldMapper,
            IndexVersion.current(),
            fieldVectors
        );

        ScoreDoc[] scoreDocs = new ScoreDoc[] {
            new ScoreDoc(1, 2.0f),
            new ScoreDoc(2, 1.8f),
            new ScoreDoc(3, 1.8f),
            new ScoreDoc(4, 1.0f),
            new ScoreDoc(5, 0.8f),
            new ScoreDoc(6, 0.8f) };

        TotalHits totalHits = new TotalHits(6L, TotalHits.Relation.EQUAL_TO);
        TopDocs topDocs = new TopDocs(totalHits, scoreDocs);

        MMRResultDiversification resultDiversification = new MMRResultDiversification();
        TopDocs diversifiedTopDocs = resultDiversification.diversify(topDocs, diversificationContext);
        assertNotSame(topDocs, diversifiedTopDocs);

        assertEquals(3, diversifiedTopDocs.scoreDocs.length);
        assertEquals(1, diversifiedTopDocs.scoreDocs[0].doc);
        assertEquals(6, diversifiedTopDocs.scoreDocs[1].doc);
        assertEquals(3, diversifiedTopDocs.scoreDocs[2].doc);
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
            queryVectorData,
            fieldMapper,
            IndexVersion.current(),
            new HashMap<>()
        );
        TopDocs emptyTopDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {});

        MMRResultDiversification resultDiversification = new MMRResultDiversification();

        assertSame(emptyTopDocs, resultDiversification.diversify(emptyTopDocs, diversificationContext));
        assertNull(resultDiversification.diversify(null, diversificationContext));
    }
}
