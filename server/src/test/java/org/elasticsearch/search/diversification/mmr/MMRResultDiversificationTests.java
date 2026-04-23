/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification.mmr;

import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class MMRResultDiversificationTests extends ESTestCase {

    public void testMMRDiversificationWithFloatVectors() throws IOException {
        List<Integer> expectedDocIds = new ArrayList<>();
        MMRResultDiversificationContext diversificationContext = getFloatContext(expectedDocIds);

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

    public void testMMRDiversificationWithByteVectors() throws IOException {
        List<Integer> expectedDocIds = new ArrayList<>();
        MMRResultDiversificationContext diversificationContext = getByteContext(expectedDocIds);

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

    private MMRResultDiversificationContext getFloatContext(List<Integer> expectedDocIds) {

        Supplier<VectorData> queryVectorData = () -> new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f });
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

        expectedDocIds.addAll(List.of(3, 4, 6));

        return diversificationContext;
    }

    private MMRResultDiversificationContext getByteContext(List<Integer> expectedDocIds) {

        Supplier<VectorData> queryVectorData = () -> new VectorData(new byte[] { 0x50, 0x20, 0x40, 0x40 });
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

        expectedDocIds.addAll(List.of(3, 4, 6));

        return diversificationContext;
    }

    public void testMMRDiversificationWithActualNullVector() throws IOException {
        Supplier<VectorData> queryVectorData = () -> null;
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.3f, 3, queryVectorData);

        Map<Integer, VectorData> vectors = new HashMap<>();
        vectors.put(1, null);
        vectors.put(2, new VectorData(new float[] { 0.4f, 0.2f, 0.4f, 0.4f }));
        vectors.put(3, new VectorData(new float[] { 0.4f, 0.2f, 0.3f, 0.3f }));
        vectors.put(4, new VectorData(new float[] { 0.4f, 0.1f, 0.3f, 0.3f }));
        diversificationContext.setFieldVectors(vectors);

        RankDoc[] docs = new RankDoc[] {
            new RankDoc(1, 2.0f, 1),
            new RankDoc(2, 1.8f, 1),
            new RankDoc(3, 1.0f, 1),
            new RankDoc(4, 0.8f, 1) };

        int rankIndex = 1;
        for (RankDoc doc : docs) {
            doc.rank = rankIndex++;
        }

        MMRResultDiversification resultDiversification = new MMRResultDiversification(diversificationContext);
        RankDoc[] diversifiedTopDocs = resultDiversification.diversify(docs);

        assertEquals(3, diversifiedTopDocs.length);
        for (RankDoc doc : diversifiedTopDocs) {
            assertNotEquals(1, doc.rank);
        }
    }

    public void testMMRDiversificationWithAllNullVectors() throws IOException {
        Supplier<VectorData> queryVectorData = () -> null;
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.3f, 3, queryVectorData);

        Map<Integer, VectorData> vectors = new HashMap<>();
        vectors.put(1, null);
        vectors.put(2, null);
        vectors.put(3, null);
        diversificationContext.setFieldVectors(vectors);

        RankDoc[] docs = new RankDoc[] { new RankDoc(1, 2.0f, 1), new RankDoc(2, 1.8f, 1), new RankDoc(3, 1.0f, 1) };

        int rankIndex = 1;
        for (RankDoc doc : docs) {
            doc.rank = rankIndex++;
        }

        MMRResultDiversification resultDiversification = new MMRResultDiversification(diversificationContext);
        RankDoc[] diversifiedTopDocs = resultDiversification.diversify(docs);

        assertEquals(0, diversifiedTopDocs.length);
    }

    public void testMMRDiversificationIfNoSearchHits() throws IOException {

        Supplier<VectorData> queryVectorData = () -> new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f });
        var diversificationContext = new MMRResultDiversificationContext("dense_vector_field", 0.6f, 10, queryVectorData);
        RankDoc[] emptyDocs = new RankDoc[0];

        MMRResultDiversification resultDiversification = new MMRResultDiversification(diversificationContext);

        assertSame(emptyDocs, resultDiversification.diversify(emptyDocs));
        assertNull(resultDiversification.diversify(null));
    }
}
