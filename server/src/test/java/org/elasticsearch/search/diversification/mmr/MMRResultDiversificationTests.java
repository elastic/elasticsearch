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
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;

import java.io.IOException;
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
        var diversificationContext = new MMRResultDiversificationContext(
            "dense_vector_field",
            0.3f,
            3,
            queryVectorData,
            fieldMapper,
            IndexVersion.current()
        );

        SearchHit[] hits = new SearchHit[] {
            generateSearchHit(1, 2.0f, 1, new float[] { 0.4f, 0.2f, 0.4f, 0.4f }),
            generateSearchHit(2, 1.8f, 2, new float[] { 0.4f, 0.2f, 0.3f, 0.3f }),
            generateSearchHit(3, 1.6f, 3, new float[] { 0.4f, 0.1f, 0.3f, 0.3f }),
            generateSearchHit(4, 1.0f, 4, new float[] { 0.1f, 0.9f, 0.5f, 0.9f }),
            generateSearchHit(5, 0.8f, 5, new float[] { 0.1f, 0.9f, 0.5f, 0.9f }),
            generateSearchHit(6, 0.8f, 6, new float[] { 0.05f, 0.05f, 0.05f, 0.05f }) };

        TotalHits totalHits = new TotalHits(6L, TotalHits.Relation.EQUAL_TO);
        SearchHits searchHits = new SearchHits(hits, totalHits, 2.0f);

        MMRResultDiversification resultDiversification = new MMRResultDiversification();
        SearchHits diversifiedHits = resultDiversification.diversify(searchHits, diversificationContext);
        assertNotSame(searchHits, diversifiedHits);

        assertEquals(3, diversifiedHits.getHits().length);
        assertEquals(1, diversifiedHits.getHits()[0].docId());
        assertEquals(6, diversifiedHits.getHits()[1].docId());
        assertEquals(3, diversifiedHits.getHits()[2].docId());
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
            IndexVersion.current()
        );
        SearchHits emptyHits = SearchHits.EMPTY_WITH_TOTAL_HITS;

        MMRResultDiversification resultDiversification = new MMRResultDiversification();

        assertSame(emptyHits, resultDiversification.diversify(emptyHits, diversificationContext));
        assertNull(resultDiversification.diversify(null, diversificationContext));
    }

    private SearchHit generateSearchHit(int docId, float score, int rank, float[] vector) {
        DocumentField docField = new DocumentField("dense_vector_field", List.of(vector));

        return new SearchHit(
            docId,
            score,
            rank,
            new Text("_" + docId),
            null,
            0L,
            0L,
            0L,
            null,
            null,
            null,
            null,
            null,
            null,
            "index",
            "alias",
            null,
            Map.of("dense_vector_field", docField),
            Map.of(),
            null
        );
    }
}
