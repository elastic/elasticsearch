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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BBQ_MIN_DIMS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class DenseVectorFieldTypeTests extends FieldTypeTestCase {
    private final boolean indexed;

    public DenseVectorFieldTypeTests() {
        this.indexed = randomBoolean();
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
                randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true))
            ),
            new DenseVectorFieldMapper.Int4HnswIndexOptions(
                randomIntBetween(1, 100),
                randomIntBetween(1, 10_000),
                randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true))
            ),
            new DenseVectorFieldMapper.FlatIndexOptions(),
            new DenseVectorFieldMapper.Int8FlatIndexOptions(randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true))),
            new DenseVectorFieldMapper.Int4FlatIndexOptions(randomFrom((Float) null, 0f, (float) randomDoubleBetween(0.9, 1.0, true))),
            new DenseVectorFieldMapper.BBQHnswIndexOptions(randomIntBetween(1, 100), randomIntBetween(1, 10_000)),
            new DenseVectorFieldMapper.BBQFlatIndexOptions()
        );
    }

    private DenseVectorFieldType createFloatFieldType() {
        return new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.FLOAT,
            BBQ_MIN_DIMS,
            indexed,
            VectorSimilarity.COSINE,
            indexed ? randomIndexOptionsAll() : null,
            Collections.emptyMap()
        );
    }

    private DenseVectorFieldType createByteFieldType() {
        return new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.BYTE,
            5,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsNonQuantized(),
            Collections.emptyMap()
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
        expectThrows(IllegalArgumentException.class, () -> fft.docValueFormat(null, null));
        DenseVectorFieldType bft = createByteFieldType();
        expectThrows(IllegalArgumentException.class, () -> bft.docValueFormat(null, null));
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
                DenseVectorFieldMapper.ElementType.FLOAT,
                dims,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsAll(),
                Collections.emptyMap()
            );
            float[] queryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomFloat();
            }
            Query query = field.createKnnQuery(VectorData.fromFloats(queryVector), 10, 10, null, null, producer);
            assertThat(query, instanceOf(DiversifyingChildrenFloatKnnVectorQuery.class));
        }
        {
            DenseVectorFieldType field = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                DenseVectorFieldMapper.ElementType.BYTE,
                dims,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsNonQuantized(),
                Collections.emptyMap()
            );
            byte[] queryVector = new byte[dims];
            float[] floatQueryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomByte();
                floatQueryVector[i] = queryVector[i];
            }
            VectorData vectorData = new VectorData(null, queryVector);
            Query query = field.createKnnQuery(vectorData, 10, 10, null, null, producer);
            assertThat(query, instanceOf(DiversifyingChildrenByteKnnVectorQuery.class));

            vectorData = new VectorData(floatQueryVector, null);
            query = field.createKnnQuery(vectorData, 10, 10, null, null, producer);
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
                DenseVectorFieldMapper.ElementType.FLOAT,
                dims,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsAll(),
                Collections.emptyMap()
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
                DenseVectorFieldMapper.ElementType.BYTE,
                dims,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsNonQuantized(),
                Collections.emptyMap()
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
            DenseVectorFieldMapper.ElementType.FLOAT,
            4,
            false,
            VectorSimilarity.COSINE,
            null,
            Collections.emptyMap()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unindexedField.createKnnQuery(VectorData.fromFloats(new float[] { 0.3f, 0.1f, 1.0f, 0.0f }), 10, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("to perform knn search on field [f], its mapping must have [index] set to [true]"));

        DenseVectorFieldType dotProductField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.FLOAT,
            BBQ_MIN_DIMS,
            true,
            VectorSimilarity.DOT_PRODUCT,
            randomIndexOptionsAll(),
            Collections.emptyMap()
        );
        float[] queryVector = new float[BBQ_MIN_DIMS];
        for (int i = 0; i < BBQ_MIN_DIMS; i++) {
            queryVector[i] = i;
        }
        e = expectThrows(
            IllegalArgumentException.class,
            () -> dotProductField.createKnnQuery(VectorData.fromFloats(queryVector), 10, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("The [dot_product] similarity can only be used with unit-length vectors."));

        DenseVectorFieldType cosineField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.FLOAT,
            BBQ_MIN_DIMS,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsAll(),
            Collections.emptyMap()
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> cosineField.createKnnQuery(VectorData.fromFloats(new float[BBQ_MIN_DIMS]), 10, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("The [cosine] similarity does not support vectors with zero magnitude."));
    }

    public void testCreateKnnQueryMaxDims() {
        {   // float type with 4096 dims
            DenseVectorFieldType fieldWith4096dims = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                DenseVectorFieldMapper.ElementType.FLOAT,
                4096,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsAll(),
                Collections.emptyMap()
            );
            float[] queryVector = new float[4096];
            for (int i = 0; i < 4096; i++) {
                queryVector[i] = randomFloat();
            }
            Query query = fieldWith4096dims.createKnnQuery(VectorData.fromFloats(queryVector), 10, 10, null, null, null);
            assertThat(query, instanceOf(KnnFloatVectorQuery.class));
        }

        {   // byte type with 4096 dims
            DenseVectorFieldType fieldWith4096dims = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                DenseVectorFieldMapper.ElementType.BYTE,
                4096,
                true,
                VectorSimilarity.COSINE,
                randomIndexOptionsNonQuantized(),
                Collections.emptyMap()
            );
            byte[] queryVector = new byte[4096];
            for (int i = 0; i < 4096; i++) {
                queryVector[i] = randomByte();
            }
            VectorData vectorData = new VectorData(null, queryVector);
            Query query = fieldWith4096dims.createKnnQuery(vectorData, 10, 10, null, null, null);
            assertThat(query, instanceOf(KnnByteVectorQuery.class));
        }
    }

    public void testByteCreateKnnQuery() {
        DenseVectorFieldType unindexedField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.BYTE,
            3,
            false,
            VectorSimilarity.COSINE,
            randomIndexOptionsNonQuantized(),
            Collections.emptyMap()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unindexedField.createKnnQuery(VectorData.fromFloats(new float[] { 0.3f, 0.1f, 1.0f }), 10, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("to perform knn search on field [f], its mapping must have [index] set to [true]"));

        DenseVectorFieldType cosineField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.BYTE,
            3,
            true,
            VectorSimilarity.COSINE,
            randomIndexOptionsNonQuantized(),
            Collections.emptyMap()
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> cosineField.createKnnQuery(VectorData.fromFloats(new float[] { 0.0f, 0.0f, 0.0f }), 10, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("The [cosine] similarity does not support vectors with zero magnitude."));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> cosineField.createKnnQuery(new VectorData(null, new byte[] { 0, 0, 0 }), 10, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("The [cosine] similarity does not support vectors with zero magnitude."));
    }
}
