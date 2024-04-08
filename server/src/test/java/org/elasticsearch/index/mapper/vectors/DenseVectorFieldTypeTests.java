/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
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
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class DenseVectorFieldTypeTests extends FieldTypeTestCase {
    private final boolean indexed;

    public DenseVectorFieldTypeTests() {
        this.indexed = randomBoolean();
    }

    private DenseVectorFieldType createFloatFieldType() {
        return new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.FLOAT,
            5,
            indexed,
            VectorSimilarity.COSINE,
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
        FieldDataContext fdc = new FieldDataContext("test", () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
        assertNotNull(fft.fielddataBuilder(fdc));

        DenseVectorFieldType bft = createByteFieldType();
        FieldDataContext bdc = new FieldDataContext("test", () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
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
        List<Double> vector = List.of(0.0, 1.0, 2.0, 3.0, 4.0);
        assertEquals(vector, fetchSourceValue(fft, vector));
        DenseVectorFieldType bft = createByteFieldType();
        assertEquals(vector, fetchSourceValue(bft, vector));
    }

    public void testCreateNestedKnnQuery() {
        BitSetProducer producer = context -> null;

        int dims = randomIntBetween(2, 2048);
        {
            DenseVectorFieldType field = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                DenseVectorFieldMapper.ElementType.FLOAT,
                dims,
                true,
                VectorSimilarity.COSINE,
                Collections.emptyMap()
            );
            float[] queryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomFloat();
            }
            Query query = field.createKnnQuery(queryVector, 10, null, null, producer);
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
                Collections.emptyMap()
            );
            byte[] queryVector = new byte[dims];
            float[] floatQueryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomByte();
                floatQueryVector[i] = queryVector[i];
            }
            Query query = field.createKnnQuery(queryVector, 10, null, null, producer);
            assertThat(query, instanceOf(DiversifyingChildrenByteKnnVectorQuery.class));

            query = field.createKnnQuery(floatQueryVector, 10, null, null, producer);
            assertThat(query, instanceOf(DiversifyingChildrenByteKnnVectorQuery.class));
        }
    }

    public void testExactKnnQuery() {
        int dims = randomIntBetween(2, 2048);
        {
            DenseVectorFieldType field = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                DenseVectorFieldMapper.ElementType.FLOAT,
                dims,
                true,
                VectorSimilarity.COSINE,
                Collections.emptyMap()
            );
            float[] queryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomFloat();
            }
            Query query = field.createExactKnnQuery(VectorData.fromFloats(queryVector));
            assertTrue(query instanceof BooleanQuery);
            BooleanQuery booleanQuery = (BooleanQuery) query;
            boolean foundFunction = false;
            for (BooleanClause clause : booleanQuery) {
                if (clause.getQuery() instanceof FunctionQuery functionQuery) {
                    foundFunction = true;
                    assertTrue(functionQuery.getValueSource() instanceof FloatVectorSimilarityFunction);
                }
            }
            assertTrue("Unable to find FloatVectorSimilarityFunction in created BooleanQuery", foundFunction);
        }
        {
            DenseVectorFieldType field = new DenseVectorFieldType(
                "f",
                IndexVersion.current(),
                DenseVectorFieldMapper.ElementType.BYTE,
                dims,
                true,
                VectorSimilarity.COSINE,
                Collections.emptyMap()
            );
            byte[] queryVector = new byte[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = randomByte();
            }
            Query query = field.createExactKnnQuery(VectorData.fromBytes(queryVector));
            assertTrue(query instanceof BooleanQuery);
            BooleanQuery booleanQuery = (BooleanQuery) query;
            boolean foundFunction = false;
            for (BooleanClause clause : booleanQuery) {
                if (clause.getQuery() instanceof FunctionQuery functionQuery) {
                    foundFunction = true;
                    assertTrue(functionQuery.getValueSource() instanceof ByteVectorSimilarityFunction);
                }
            }
            assertTrue("Unable to find FloatVectorSimilarityFunction in created BooleanQuery", foundFunction);
        }
    }

    public void testFloatCreateKnnQuery() {
        DenseVectorFieldType unindexedField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.FLOAT,
            3,
            false,
            VectorSimilarity.COSINE,
            Collections.emptyMap()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unindexedField.createKnnQuery(new float[] { 0.3f, 0.1f, 1.0f }, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("to perform knn search on field [f], its mapping must have [index] set to [true]"));

        DenseVectorFieldType dotProductField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.FLOAT,
            3,
            true,
            VectorSimilarity.DOT_PRODUCT,
            Collections.emptyMap()
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> dotProductField.createKnnQuery(new float[] { 0.3f, 0.1f, 1.0f }, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("The [dot_product] similarity can only be used with unit-length vectors."));

        DenseVectorFieldType cosineField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.FLOAT,
            3,
            true,
            VectorSimilarity.COSINE,
            Collections.emptyMap()
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> cosineField.createKnnQuery(new float[] { 0.0f, 0.0f, 0.0f }, 10, null, null, null)
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
                Collections.emptyMap()
            );
            float[] queryVector = new float[4096];
            for (int i = 0; i < 4096; i++) {
                queryVector[i] = randomFloat();
            }
            Query query = fieldWith4096dims.createKnnQuery(queryVector, 10, null, null, null);
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
                Collections.emptyMap()
            );
            byte[] queryVector = new byte[4096];
            for (int i = 0; i < 4096; i++) {
                queryVector[i] = randomByte();
            }
            Query query = fieldWith4096dims.createKnnQuery(queryVector, 10, null, null, null);
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
            Collections.emptyMap()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unindexedField.createKnnQuery(new float[] { 0.3f, 0.1f, 1.0f }, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("to perform knn search on field [f], its mapping must have [index] set to [true]"));

        DenseVectorFieldType cosineField = new DenseVectorFieldType(
            "f",
            IndexVersion.current(),
            DenseVectorFieldMapper.ElementType.BYTE,
            3,
            true,
            VectorSimilarity.COSINE,
            Collections.emptyMap()
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> cosineField.createKnnQuery(new float[] { 0.0f, 0.0f, 0.0f }, 10, null, null, null)
        );
        assertThat(e.getMessage(), containsString("The [cosine] similarity does not support vectors with zero magnitude."));

        e = expectThrows(IllegalArgumentException.class, () -> cosineField.createKnnQuery(new byte[] { 0, 0, 0 }, 10, null, null, null));
        assertThat(e.getMessage(), containsString("The [cosine] similarity does not support vectors with zero magnitude."));
    }
}
