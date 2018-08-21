/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SparseVectorFieldMapper;
import org.elasticsearch.common.xcontent.ObjectParser;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * Query to run on a [vector] field.
 */
public final class VectorQueryBuilder extends AbstractQueryBuilder<VectorQueryBuilder> {
    public static final String NAME = "vector";
    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");

    private static final ObjectParser<VectorQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, true, VectorQueryBuilder::new);
    static {
        PARSER.declareString(VectorQueryBuilder::setField, FIELD_FIELD);
        PARSER.declareField(VectorQueryBuilder::setVector, parser -> parser, QUERY_VECTOR_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    private String field = null;
    private float[] queryVector = null;
    private int[] queryVectorDims = null;

    public VectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.queryVector = null;
        this.queryVectorDims = null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        //out.writeList(queryVector);
    }

    public static VectorQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    // constuctor used by the parser
    private VectorQueryBuilder() {};

    private VectorQueryBuilder setField(String field) {
        this.field = field;
        return this;
    }

    private VectorQueryBuilder setVector(XContentParser parser) {
        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            return setDenseVector(parser);
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return  setSparseVector(parser);
        }
        throw new IllegalArgumentException("[vector] query requires a non-empty [query_vector] as an array or a map! ");
    }

    // parse dense array from XContent
    private VectorQueryBuilder setDenseVector(XContentParser parser) {
        try {
            float[] vector = new float[10]; //initialize with default of 10 dims
            XContentParser.Token token;
            int dimCount = 0;
            for (token = parser.nextToken(); token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (vector.length <= dimCount) vector = ArrayUtil.grow(vector);
                    vector[dimCount] = parser.floatValue(true);
                    dimCount++;
                } else {
                    throw new IllegalArgumentException("[vector] field takes an array of floats, but got unexpected token " + token);
                }
            }
            if (dimCount == 0 ) {
                throw new IllegalArgumentException("[vector] query requires a non-empty [query_vector] as an array or a map! ");
            }
            if (vector.length > dimCount) {
                vector = Arrays.copyOf(vector, dimCount); //shrink array to dimCount, as it may be over-located
            }
            this.queryVector = vector;
            return this;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }

    }

    // parse sparse array from XContent
    private VectorQueryBuilder setSparseVector(XContentParser parser) {
        try {
            float[] vector = new float[10]; //initialize with default of 10 dims
            int[] dims = new int[10];
            XContentParser.Token token;
            int dimCount = 0;
            int dim = 0;
            for (token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    dim = Integer.parseInt(parser.currentName());
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    float value = parser.floatValue(true);
                    if (dims.length <= dimCount) { // ensure arrays have enough capacity
                        ArrayUtil.grow(vector, dimCount + 1);
                        ArrayUtil.grow(dims, dimCount + 1);
                    }
                    SparseVectorFieldMapper.sortedInsertDimValue(vector, dims, value, dim, dimCount);
                    dimCount ++;
                } else {
                    throw new IllegalArgumentException("[vector] field takes an object that " + "" +
                        "maps a dimension number [int] to a value [float], but got unexpected token " + token);
                }
            }
            if (dimCount == 0 ) {
                throw new IllegalArgumentException("[vector] query requires a non-empty [query_vector] as an array or a map! ");
            }
            if (vector.length > dimCount) {
                vector = Arrays.copyOf(vector, dimCount); //shrink array to dimCount, as it may be over-located
                dims = Arrays.copyOf(dims, dimCount);
            }
            this.queryVector = vector;
            this.queryVectorDims = dims;
            return this;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }


    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field("field", field);
        // for queryVector
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(field);
        if (ft instanceof DenseVectorFieldMapper.DenseVectorFieldType) {
            return new VectorQuery(field, queryVector, queryVectorDims, true);
        } else if (ft instanceof SparseVectorFieldMapper.SparseVectorFieldType) {
            return new VectorQuery(field, queryVector, queryVectorDims, false);
        } else {
            throw new IllegalArgumentException("[vector] query only works on [dense_vector] or [sparse_vector] fields, " +
                "not [" + ft.typeName() + "]");
        }
    }

    @Override
    protected boolean doEquals(VectorQueryBuilder other) {
        return Objects.equals(field, other.field) &&
            Arrays.equals(queryVector, other.queryVector) &&
            Arrays.equals(queryVectorDims, other.queryVectorDims);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Arrays.hashCode(queryVector), Arrays.hashCode(queryVectorDims));
    }


    public static final class VectorQuery extends Query {
        private final String fieldName;
        private final float[] queryVector;
        private final int[] queryVectorDims;
        private final boolean isDocVectorDense;
        private final float queryVectorMagnitude;

        VectorQuery(String fieldName, float[] queryVector, int[] queryVectorDims, boolean isDocVectorDense) {
            this.fieldName = fieldName;
            this.queryVector = queryVector;
            this.queryVectorDims = queryVectorDims;
            this.isDocVectorDense = isDocVectorDense;
            float dotProduct = 0f;
            for (int dim = 0; dim < queryVector.length; dim++) {
                dotProduct += queryVector[dim] * queryVector[dim];
            }
            this.queryVectorMagnitude = (float) Math.sqrt(dotProduct);
        }

        @Override
        public boolean equals(Object obj) {
            if ((obj instanceof VectorQuery) == false) return false;
            VectorQuery that = (VectorQuery) obj;
            return Objects.equals(fieldName, that.fieldName) &&
                Arrays.equals(queryVector, that.queryVector) &&
                Arrays.equals(queryVectorDims, that.queryVectorDims) &&
                isDocVectorDense == that.isDocVectorDense;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, Arrays.hashCode(queryVector), Arrays.hashCode(queryVectorDims), isDocVectorDense);
        }

        @Override
        public String toString(String field) {
            return "VectorQuery(field=" + fieldName + ", query_vector of dim=" + queryVector.length  +")";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
            return new Weight(this) {

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }

                @Override
                public void extractTerms(Set<Term> terms) {}

                // TODO: fill explain section
                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    return null;
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    BinaryDocValues vectorDVs = context.reader().getBinaryDocValues(fieldName);
                    if (vectorDVs == null) return null;
                    return new Scorer(this) {
                        @Override
                        public int docID() {
                            return vectorDVs.docID();
                        }
                        @Override
                        public float score() throws IOException {
                            BytesRef vectorBR = vectorDVs.binaryValue();
                            float[] docVector = DenseVectorFieldMapper.decodeVector(vectorBR);
                            int[] docVectorDims;
                            if (isDocVectorDense) {
                                docVectorDims = null;
                            } else {
                                docVectorDims = SparseVectorFieldMapper.decodeVectorDims(vectorBR, docVector.length);
                            }
                            return cosineSimilarity(docVector, docVectorDims,  queryVector, queryVectorDims, queryVectorMagnitude);
                        }
                        @Override
                        public DocIdSetIterator iterator() {
                            return vectorDVs;
                        }
                    };
                }

            };
        }

    }


    //**************STATIC HELPER METHODS************************************

    /**
     * Calculates cosine similarity between vectors v1 and v2
     * Vectors can be sparse or dense
     * @param v1Values - values for v1 vector
     * @param v1Dims - dimensions for v1 vector, or null of v1 vector is dense
     * @param v2Values - values for v2 vector
     * @param v2Dims - dimensions for v2 vector, or null of v2 vector is dense
     * @param v2Magnitude - magnitude of v2 vector
     * @return cosine similarity between vectors
     */
    private static float cosineSimilarity(float[] v1Values, int[] v1Dims, float[] v2Values, int[] v2Dims, float v2Magnitude) {
        float v1DotProduct = 0f;
        float v1v2DotProduct = 0f;
        int v1Index = 0; int v1Dim = 0;
        int v2Index = 0; int v2Dim = 0;
        // find common dimensions among vectors v1 and v2 and calculate dotProduct based on common dimensions
        while (v1Index < v1Values.length && v2Index < v2Values.length) {
            // if a vector is dense, its dimensions array is null, and its dimensions are indexes of its values array
            // if a vector is sparse, its dimensions are in its dimensions array
            v1Dim = v1Dims == null ? v1Index : v1Dims[v1Index];
            v2Dim = v2Dims == null ? v2Index : v2Dims[v2Index];
            if (v1Dim == v2Dim) {
                v1v2DotProduct += v1Values[v1Index] * v2Values[v2Index];
                v1Index++;
                v2Index++;
            } else if (v1Dim > v2Dim) {
                v2Index++;
            } else {
                v1Index++;
            }
        }
        // calculate docProduct of vector v1 by itself
        for (int dim = 0; dim < v1Values.length; dim++) {
            v1DotProduct += v1Values[dim] * v1Values[dim];
        }
        final float v1Magnitude = (float) Math.sqrt(v1DotProduct);
        return v1v2DotProduct / (v1Magnitude * v2Magnitude);
    }
}
