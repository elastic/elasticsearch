/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.PatienceKnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.ESKnnByteVectorQuery;
import org.elasticsearch.search.vectors.ESKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.RescoreKnnVectorQuery;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BBQ_MIN_DIMS;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType.BIT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType.BYTE;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType.FLOAT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.OVERSAMPLE_LIMIT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DenseVectorFieldTypeTests extends FieldTypeTestCase {
    private final boolean indexed;

    public DenseVectorFieldTypeTests() {
        this.indexed = randomBoolean();
    }

    private static DenseVectorFieldMapper.RescoreVector randomRescoreVector() {
        return new DenseVectorFieldMapper.RescoreVector(randomBoolean() ? 0 : randomFloatBetween(1.0F, 10.0F, false));
    }

    private DenseVectorFieldMapper.IndexOptions randomIndexOptionsNonQuantized() {
        return randomFrom(
            new DenseVectorFieldMapper.HnswIndexOptions(randomIntBetween(1, 100), randomIntBetween(1, 10_000)),
            new DenseVectorFieldMapper.FlatIndexOptions()
        );
    }

    private DenseVectorFieldMapper.IndexOptions randomIndexOptionsAll() {
        return randomFrom(
            new DenseVectorFieldMapper.HnswIndexOptions(randomIntBetween(1, 100), randomIntBetween(1, 10_000)),
            new DenseVectorFieldMapper.Int8HnswIndexOptions(
                randomIntBetween(1, 100),
                randomIntBetween(1, 10_000),
                randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true)),
                randomFrom((DenseVectorFieldMapper.RescoreVector) null, randomRescoreVector()),
                randomBoolean()
            ),
            new DenseVectorFieldMapper.Int4HnswIndexOptions(
                randomIntBetween(1, 100),
                randomIntBetween(1, 10_000),
                randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true)),
                randomFrom((DenseVectorFieldMapper.RescoreVector) null, randomRescoreVector()),
                randomBoolean()
            ),
            new DenseVectorFieldMapper.FlatIndexOptions(),
            new DenseVectorFieldMapper.Int8FlatIndexOptions(
                randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true)),
                randomFrom((DenseVectorFieldMapper.RescoreVector) null, randomRescoreVector())
            ),
            new DenseVectorFieldMapper.Int4FlatIndexOptions(
                randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true)),
                randomFrom((DenseVectorFieldMapper.RescoreVector) null, randomRescoreVector())
            ),
            new DenseVectorFieldMapper.BBQHnswIndexOptions(
                randomIntBetween(1, 100),
                randomIntBetween(1, 10_000),
                randomFrom((DenseVectorFieldMapper.RescoreVector) null, randomRescoreVector()),
                randomBoolean()
            ),
            new DenseVectorFieldMapper.BBQFlatIndexOptions(randomFrom((DenseVectorFieldMapper.RescoreVector) null, randomRescoreVector()))
        );
    }

    private DenseVectorFieldMapper.IndexOptions randomIndexOptionsHnswQuantized() {
        return randomIndexOptionsHnswQuantized(randomBoolean() ? null : randomRescoreVector());
    }

    private DenseVectorFieldMapper.IndexOptions randomIndexOptionsHnswQuantized(DenseVectorFieldMapper.RescoreVector rescoreVector) {
        return randomFrom(
            new DenseVectorFieldMapper.Int8HnswIndexOptions(
                randomIntBetween(1, 100),
                randomIntBetween(1, 10_000),
                randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true)),
                rescoreVector,
                randomBoolean()
            ),
            new DenseVectorFieldMapper.Int4HnswIndexOptions(
                randomIntBetween(1, 100),
                randomIntBetween(1, 10_000),
                randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true)),
                rescoreVector,
                randomBoolean()
            ),
            new DenseVectorFieldMapper.BBQHnswIndexOptions(randomIntBetween(1, 100), randomIntBetween(1, 10_000), rescoreVector)
        );
    }

    private DenseVectorFieldType createFloatFieldType() {
        return new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            FLOAT,
            BBQ_MIN_DIMS,
            indexed,
            VectorSimilarity.COSINE,
            indexed ? randomIndexOptionsAll() : null,
            Collections.emptyMap(),
            false
        );
    }

    private DenseVectorFieldType createByteFieldType() {
        return new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            BYTE,
            5,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsNonQuantized(),
            Collections.emptyMap(),
            false
        );
    }

    public void testHasDocValues() {
        DenseVectorFieldType fft = createFloatFieldType();
        assertNotEquals(indexed, fft.hasDocValues());
        DenseVectorFieldType bft = createByteFieldType();
        assertFalse(bft.hasDocValues());
    }

    public void testIsIndexed() {
        DenseVectorFieldType fft = createFloatFieldType();
        assertEquals(indexed, fft.isIndexed());
        DenseVectorFieldType bft = createByteFieldType();
        assertTrue(bft.isIndexed());
    }

    public void testIsSearchable() {
        DenseVectorFieldType fft = createFloatFieldType();
        assertEquals(indexed, fft.isSearchable());
        DenseVectorFieldType bft = createByteFieldType();
        assertTrue(bft.isSearchable());
    }

    public void testIsAggregatable() {
        DenseVectorFieldType fft = createFloatFieldType();
        assertFalse(fft.isAggregatable());
        DenseVectorFieldType bft = createByteFieldType();
        assertFalse(bft.isAggregatable());
    }

    public void testFielddataBuilder() {
        DenseVectorFieldType fft = createFloatFieldType();
        FieldDataContext fdc = new FieldDataContext("test", null, () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
        assertNotNull(fft.fielddataBuilder(fdc));

        DenseVectorFieldType bft = createByteFieldType();
        FieldDataContext bdc = new FieldDataContext("test", null, () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
        assertNotNull(bft.fielddataBuilder(bdc));
    }

    public void testDocValueFormat() {
        DenseVectorFieldType fft = createFloatFieldType();
        assertEquals(DocValueFormat.DENSE_VECTOR, fft.docValueFormat(null, null));
        DenseVectorFieldType bft = createByteFieldType();
        assertEquals(DocValueFormat.DENSE_VECTOR, bft.docValueFormat(null, null));
    }

    public void testFetchSourceValue() throws IOException {
        DenseVectorFieldType fft = createFloatFieldType();
        List<Double> vector = List.of(0.0, 1.0, 2.0, 3.0, 4.0, 6.0);
        assertEquals(vector, fetchSourceValue(fft, vector));
        DenseVectorFieldType bft = createByteFieldType();
        assertEquals(vector, fetchSourceValue(bft, vector));
    }

    public void testCreateNestedKnnQuery() {
        BitSetProducer producer = context -> null;

        int dims = randomIntBetween(BBQ_MIN_DIMS, 2048);
        if (dims % 2 != 0) {
            dims++;
        }
        {
            DenseVectorFieldType field = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                FLOAT,
                dims,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsAll(),
                Collections.emptyMap(),
                false
            );
            float[] queryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomFloat();
            }
            Query query = field.createKnnQuery(
                VectorData.fromFloats(queryVector),
                10,
                10,
                null,
                null,
                null,
                producer,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            );
            if (query instanceof RescoreKnnVectorQuery rescoreKnnVectorQuery) {
                query = rescoreKnnVectorQuery.innerQuery();
            }
            assertThat(query, instanceOf(DiversifyingChildrenFloatKnnVectorQuery.class));
        }
        {
            DenseVectorFieldType field = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                BYTE,
                dims,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsNonQuantized(),
                Collections.emptyMap(),
                false
            );
            byte[] queryVector = new byte[dims];
            float[] floatQueryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomByte();
                floatQueryVector[i] = queryVector[i];
            }
            VectorData vectorData = new VectorData(null, queryVector);
            Query query = field.createKnnQuery(
                vectorData,
                10,
                10,
                null,
                null,
                null,
                producer,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            );
            assertThat(query, instanceOf(DiversifyingChildrenByteKnnVectorQuery.class));

            vectorData = new VectorData(floatQueryVector, null);
            query = field.createKnnQuery(
                vectorData,
                10,
                10,
                null,
                null,
                null,
                producer,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            );
            assertThat(query, instanceOf(DiversifyingChildrenByteKnnVectorQuery.class));
        }
    }

    public void testExactKnnQuery() {
        int dims = randomIntBetween(BBQ_MIN_DIMS, 2048);
        if (dims % 2 != 0) {
            dims++;
        }
        {
            DenseVectorFieldType field = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                FLOAT,
                dims,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsAll(),
                Collections.emptyMap(),
                false
            );
            float[] queryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomFloat();
            }
            Query query = field.createExactKnnQuery(VectorData.fromFloats(queryVector), null);
            assertTrue(query instanceof DenseVectorQuery.Floats);
        }
        {
            DenseVectorFieldType field = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                BYTE,
                dims,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsNonQuantized(),
                Collections.emptyMap(),
                false
            );
            byte[] queryVector = new byte[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomByte();
            }
            Query query = field.createExactKnnQuery(VectorData.fromBytes(queryVector), null);
            assertTrue(query instanceof DenseVectorQuery.Bytes);
        }
    }

    public void testFloatCreateKnnQuery() {
        DenseVectorFieldType unindexedField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            FLOAT,
            4,
            false,
            VectorSimilarity.COSINE,
            null,
            Collections.emptyMap(),
            false
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unindexedField.createKnnQuery(
                VectorData.fromFloats(new float[] { 0.3f, 0.1f, 1.0f, 0.0f }),
                10,
                10,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(e.getMessage(), containsString("to perform knn search on field [f], its mapping must have [index] set to [true]"));

        DenseVectorFieldType dotProductField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            FLOAT,
            BBQ_MIN_DIMS,
            true,
            VectorSimilarity.DOT_PRODUCT,
            randomIndexOptionsAll(),
            Collections.emptyMap(),
            false
        );
        float[] queryVector = new float[BBQ_MIN_DIMS];
        for (int i = 0; i < BBQ_MIN_DIMS; i++) {
            queryVector[i] = i;
        }
        e = expectThrows(
            IllegalArgumentException.class,
            () -> dotProductField.createKnnQuery(
                VectorData.fromFloats(queryVector),
                10,
                10,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(e.getMessage(), containsString("The [dot_product] similarity can only be used with unit-length vectors."));

        DenseVectorFieldType cosineField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            FLOAT,
            BBQ_MIN_DIMS,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsAll(),
            Collections.emptyMap(),
            false
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> cosineField.createKnnQuery(
                VectorData.fromFloats(new float[BBQ_MIN_DIMS]),
                10,
                10,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(e.getMessage(), containsString("The [cosine] similarity does not support vectors with zero magnitude."));
    }

    public void testCreateKnnQueryMaxDims() {
        {   // float type with 4096 dims
            DenseVectorFieldType fieldWith4096dims = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                FLOAT,
                4096,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsAll(),
                Collections.emptyMap(),
                false
            );
            float[] queryVector = new float[4096];
            for (int i = 0; i < 4096; i++) {
                queryVector[i] = randomFloat();
            }
            Query query = fieldWith4096dims.createKnnQuery(
                VectorData.fromFloats(queryVector),
                10,
                10,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            );
            if (query instanceof RescoreKnnVectorQuery rescoreKnnVectorQuery) {
                query = rescoreKnnVectorQuery.innerQuery();
            }
            assertThat(query, instanceOf(KnnFloatVectorQuery.class));
        }

        {   // byte type with 4096 dims
            DenseVectorFieldType fieldWith4096dims = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                BYTE,
                4096,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsNonQuantized(),
                Collections.emptyMap(),
                false
            );
            byte[] queryVector = new byte[4096];
            for (int i = 0; i < 4096; i++) {
                queryVector[i] = randomByte();
            }
            VectorData vectorData = new VectorData(null, queryVector);
            Query query = fieldWith4096dims.createKnnQuery(
                vectorData,
                10,
                10,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            );
            assertThat(query, instanceOf(ESKnnByteVectorQuery.class));
        }
    }

    public void testByteCreateKnnQuery() {
        DenseVectorFieldType unindexedField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            BYTE,
            3,
            false,
            VectorSimilarity.COSINE,
            randomIndexOptionsNonQuantized(),
            Collections.emptyMap(),
            false
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unindexedField.createKnnQuery(
                VectorData.fromFloats(new float[] { 0.3f, 0.1f, 1.0f }),
                10,
                10,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(e.getMessage(), containsString("to perform knn search on field [f], its mapping must have [index] set to [true]"));

        DenseVectorFieldType cosineField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            BYTE,
            3,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsNonQuantized(),
            Collections.emptyMap(),
            false
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> cosineField.createKnnQuery(
                VectorData.fromFloats(new float[] { 0.0f, 0.0f, 0.0f }),
                10,
                10,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(e.getMessage(), containsString("The [cosine] similarity does not support vectors with zero magnitude."));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> cosineField.createKnnQuery(
                new VectorData(null, new byte[] { 0, 0, 0 }),
                10,
                10,
                null,
                null,
                null,
                null,
                randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
            )
        );
        assertThat(e.getMessage(), containsString("The [cosine] similarity does not support vectors with zero magnitude."));
    }

    public void testRescoreOversampleUsedWithoutQuantization() {
        DenseVectorFieldMapper.ElementType elementType = randomFrom(FLOAT, BYTE);
        DenseVectorFieldType nonQuantizedField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            elementType,
            3,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsNonQuantized(),
            Collections.emptyMap(),
            false
        );

        Query knnQuery = nonQuantizedField.createKnnQuery(
            new VectorData(null, new byte[] { 1, 4, 10 }),
            10,
            100,
            randomFloatBetween(1.0F, 10.0F, false),
            null,
            null,
            null,
            randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
        );

        if (elementType == BYTE) {
            if (knnQuery instanceof PatienceKnnVectorQuery patienceKnnVectorQuery) {
                assertThat(patienceKnnVectorQuery.getK(), is(100));
            } else {
                ESKnnByteVectorQuery knnByteVectorQuery = (ESKnnByteVectorQuery) knnQuery;
                assertThat(knnByteVectorQuery.getK(), is(100));
                assertThat(knnByteVectorQuery.kParam(), is(10));
            }
        } else {
            if (knnQuery instanceof PatienceKnnVectorQuery patienceKnnVectorQuery) {
                assertThat(patienceKnnVectorQuery.getK(), is(100));
            } else {
                ESKnnFloatVectorQuery knnFloatVectorQuery = (ESKnnFloatVectorQuery) knnQuery;
                assertThat(knnFloatVectorQuery.getK(), is(100));
                assertThat(knnFloatVectorQuery.kParam(), is(10));
            }
        }
    }

    public void testRescoreOversampleModifiesNumCandidates() {
        DenseVectorFieldType fieldType = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            FLOAT,
            3,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsHnswQuantized(),
            Collections.emptyMap(),
            false
        );

        // Total results is k, internal k is multiplied by oversample
        checkRescoreQueryParameters(fieldType, 10, 200, 2.5F, 25, 200, 10);
        // If numCands < k, update numCands to k
        checkRescoreQueryParameters(fieldType, 10, 20, 2.5F, 25, 25, 10);
        // Oversampling limits for k
        checkRescoreQueryParameters(fieldType, 1000, 1000, 11.0F, OVERSAMPLE_LIMIT, OVERSAMPLE_LIMIT, 1000);
    }

    public void testRescoreOversampleQueryOverrides() {
        // verify we can override to `0`
        DenseVectorFieldType fieldType = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            FLOAT,
            3,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsHnswQuantized(new DenseVectorFieldMapper.RescoreVector(randomFloatBetween(1.1f, 9.9f, false))),
            Collections.emptyMap(),
            false
        );
        Query query = fieldType.createKnnQuery(
            VectorData.fromFloats(new float[] { 1, 4, 10 }),
            10,
            100,
            0f,
            null,
            null,
            null,
            randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
        );
        assertTrue(query instanceof ESKnnFloatVectorQuery);

        // verify we can override a `0` to a positive number
        fieldType = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            FLOAT,
            3,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsHnswQuantized(new DenseVectorFieldMapper.RescoreVector(0)),
            Collections.emptyMap(),
            false
        );
        query = fieldType.createKnnQuery(
            VectorData.fromFloats(new float[] { 1, 4, 10 }),
            10,
            100,
            2f,
            null,
            null,
            null,
            randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
        );
        assertTrue(query instanceof RescoreKnnVectorQuery);
        assertThat(((RescoreKnnVectorQuery) query).k(), equalTo(10));
        KnnFloatVectorQuery esKnnQuery = (KnnFloatVectorQuery) ((RescoreKnnVectorQuery) query).innerQuery();
        if (esKnnQuery instanceof ESKnnFloatVectorQuery esKnnFloatVectorQuery) {
            assertThat(esKnnFloatVectorQuery.kParam(), equalTo(20));
        }

    }

    public void testFilterSearchThreshold() {
        List<Tuple<DenseVectorFieldMapper.ElementType, Function<Query, KnnSearchStrategy>>> cases = List.of(
            Tuple.tuple(FLOAT, q -> ((ESKnnFloatVectorQuery) q).getStrategy()),
            Tuple.tuple(BYTE, q -> ((ESKnnByteVectorQuery) q).getStrategy()),
            Tuple.tuple(BIT, q -> ((ESKnnByteVectorQuery) q).getStrategy())
        );
        for (var tuple : cases) {
            DenseVectorFieldType fieldType = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                tuple.v1(),
                tuple.v1() == BIT ? 3 * 8 : 3,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsHnswQuantized(),
                Collections.emptyMap(),
                false
            );

            // Test with a filter search threshold
            Query query = fieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 1, 4, 10 }),
                10,
                100,
                0f,
                null,
                null,
                null,
                DenseVectorFieldMapper.FilterHeuristic.FANOUT
            );
            KnnSearchStrategy strategy = tuple.v2().apply(query);
            assertTrue(strategy instanceof KnnSearchStrategy.Hnsw);
            assertThat(((KnnSearchStrategy.Hnsw) strategy).filteredSearchThreshold(), equalTo(0));

            query = fieldType.createKnnQuery(
                VectorData.fromFloats(new float[] { 1, 4, 10 }),
                10,
                100,
                0f,
                null,
                null,
                null,
                DenseVectorFieldMapper.FilterHeuristic.ACORN
            );
            strategy = tuple.v2().apply(query);
            assertTrue(strategy instanceof KnnSearchStrategy.Hnsw);
            assertThat(((KnnSearchStrategy.Hnsw) strategy).filteredSearchThreshold(), equalTo(60));
        }
    }

    private static void checkRescoreQueryParameters(
        DenseVectorFieldType fieldType,
        int k,
        int candidates,
        float oversample,
        Integer expectedK,
        int expectedCandidates,
        int expectedResults
    ) {
        Query query = fieldType.createKnnQuery(
            VectorData.fromFloats(new float[] { 1, 4, 10 }),
            k,
            candidates,
            oversample,
            null,
            null,
            null,
            randomFrom(DenseVectorFieldMapper.FilterHeuristic.values())
        );
        RescoreKnnVectorQuery rescoreQuery = (RescoreKnnVectorQuery) query;
        Query innerQuery = rescoreQuery.innerQuery();
        if (innerQuery instanceof PatienceKnnVectorQuery patienceKnnVectorQuery) {
            assertThat("Unexpected candidates", patienceKnnVectorQuery.getK(), equalTo(expectedCandidates));
        } else {
            ESKnnFloatVectorQuery knnQuery = (ESKnnFloatVectorQuery) innerQuery;
            assertThat("Unexpected total results", rescoreQuery.k(), equalTo(expectedResults));
            assertThat("Unexpected candidates", knnQuery.getK(), equalTo(expectedCandidates));
            assertThat("Unexpected k parameter", knnQuery.kParam(), equalTo(expectedK));
        }
    }
}
