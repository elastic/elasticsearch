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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A {@link FieldMapper} for indexing a sparse vector of floats.
 */

public class SparseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "sparse_vector";
    public static int MAX_DIMS_COUNT = 500; //maximum allowed number of dimensions
    public static int MAX_DIMS_NUMBER = 65000; //maximum allowed dimension's number
    private static final int INT_BYTES = Integer.BYTES;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new SparseVectorFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, SparseVectorFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public SparseVectorFieldType fieldType() {
            return (SparseVectorFieldType) super.fieldType();
        }

        @Override
        public SparseVectorFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new SparseVectorFieldMapper(
                    name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            SparseVectorFieldMapper.Builder builder = new SparseVectorFieldMapper.Builder(name);
            return builder;
        }
    }

    public static final class SparseVectorFieldType extends MappedFieldType {

        public SparseVectorFieldType() {}

        protected SparseVectorFieldType(SparseVectorFieldType ref) {
            super(ref);
        }

        public SparseVectorFieldType clone() {
            return new SparseVectorFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public DocValueFormat docValueFormat(String format, DateTimeZone timeZone) {
            throw new UnsupportedOperationException("[sparse_vector] field doesn't support doc values");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            throw new UnsupportedOperationException("[sparse_vector] field doesn't support sorting, scripting or aggregating");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("Queries on [sparse_vector] fields are not supported");
        }
    }


    private SparseVectorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                    Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
    }

    @Override
    protected SparseVectorFieldMapper clone() {
        return (SparseVectorFieldMapper) super.clone();
    }

    @Override
    public SparseVectorFieldType fieldType() {
        return (SparseVectorFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("[sparse_vector] field can't be used in multi-fields");
        }
        if (context.parser().currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException("[sparse_vector] fields must be json objects, expected a START_OBJECT but got: " +
                context.parser().currentToken());
        }
        int[] dims = new int[0];
        float[] values = new float[0];
        int dimCount = 0;
        int dim = 0;
        float value;
        for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
            if (token == Token.FIELD_NAME) {
                try {
                    dim = Integer.parseInt(context.parser().currentName());
                    if (dim < 0 || dim > MAX_DIMS_NUMBER) {
                        throw new IllegalArgumentException("[sparse_vector]'s dimension number must be " +
                            "a non-negative integer value not exceeding [" + MAX_DIMS_NUMBER + "]");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("[sparse_vector]'s dimensions should be integers represented as strings, but got ["
                        + context.parser().currentName() + "]");
                }
            } else if (token == Token.VALUE_NUMBER) {
                value = context.parser().floatValue(true);
                if (dims.length <= dimCount) { // ensure arrays have enough capacity
                    values = ArrayUtil.grow(values, dimCount + 1);
                    dims = ArrayUtil.grow(dims, dimCount + 1);
                }
                dims[dimCount] = dim;
                values[dimCount] = value;
                dimCount ++;
                if (dimCount >= MAX_DIMS_COUNT) {
                    throw new IllegalArgumentException(
                        "[sparse_vector] field has exceeded the maximum allowed number of dimensions of :[" + MAX_DIMS_COUNT + "]");
                }
            } else {
                throw new IllegalArgumentException("[sparse_vector] field takes an object that maps a dimension number to a float, " +
                    "but got unexpected token [" + token + "]");
            }
        }

        BytesRef br = encodeSparseVector(values, dims, dimCount);
        BinaryDocValuesField field = new BinaryDocValuesField(fieldType().name(), br);
        context.doc().addWithKey(fieldType().name(), field);
    }


    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) {
        throw new AssertionError("parse is implemented directly");
    }


    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }


    //*************STATIC HELPER METHODS******************************

    // insert dim into the sorted array of dims
    // insert value in the array of values in the same position as the corresponding dim
    public static void sortedInsertDimValue(float[] values, int[] dims, float value, int dim, final int dimIndex) {
        int newDimIndex = dimIndex;
        while (newDimIndex > 0 && (dims[newDimIndex - 1] > dim)) {
            // move higher valued dimension to the right
            dims[newDimIndex] = dims[newDimIndex - 1] ;
            values[newDimIndex] = values[newDimIndex - 1] ;
            newDimIndex --;
        }
        dims[newDimIndex] = dim;
        values[newDimIndex] = value;
    }

    /**
     * Encodes a sparse array represented by values, dims and dimCount into a bytes array - BytesRef
     * BytesRef: int[] floats encoded as integers values, 2 bytes for each dimension
     * @param values - values of the sparse array
     * @param dims - dims of the sparse array
     * @param dimCount - number of the dimension
     * @return BytesRef
     */
    private static BytesRef encodeSparseVector(float[] values, int[] dims, int dimCount) {
        // 1. Sort dimensions in the ascending order and sort values in the same order as their corresponding dimensions
        // as we expect that data should be already provided as sorted by dim,
        // we expect just a single pass in this bubble sort algorithm
        int temp;
        float tempValue;
        int n = dimCount;
        boolean swapped = true;
        while (swapped) {
            swapped = false;
            for (int i = 1; i <= n-1; i++){
                if (dims[i-1] > dims[i]) {
                    swapped = true;
                    temp = dims[i];
                    dims[i] = dims[i-1];
                    dims[i-1] = temp;
                    tempValue = values[i];
                    values[i] = values[i-1];
                    values[i-1] = tempValue;
                }
            }
            n = n - 1;
        }

        byte[] buf = new byte[dimCount * (INT_BYTES + 2)];

        // 2. Encode values
        int offset = 0;
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = Float.floatToIntBits(values[dim]);
            buf[offset] =  (byte) (intValue >> 24);
            buf[offset+1] = (byte) (intValue >> 16);
            buf[offset+2] = (byte) (intValue >>  8);
            buf[offset+3] = (byte) intValue;
            offset += INT_BYTES;
        }

        // 3. Encode dimensions
        // as each dimension is a positive value that doesn't exceed 65000, 2 bytes is enough for encoding it
        for (int dim = 0; dim < dimCount; dim++) {
            buf[offset] = (byte) (dims[dim] >>  8);
            buf[offset+1] = (byte) dims[dim];
            offset += 2;
        }
        return new BytesRef(buf);
    }


    // Decodes the first part of the BytesRef into vector values
    public static float[] decodeVector(BytesRef vectorBR) {
        int dimCount = (vectorBR.length - vectorBR.offset) / (INT_BYTES + 2);
        float[] vector = new float[dimCount];
        int offset = vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = ((vectorBR.bytes[offset] & 0xFF) << 24)   |
                ((vectorBR.bytes[offset+1] & 0xFF) << 16) |
                ((vectorBR.bytes[offset+2] & 0xFF) <<  8) |
                (vectorBR.bytes[offset+3] & 0xFF);
            vector[dim] = Float.intBitsToFloat(intValue);
            offset = offset + INT_BYTES;
        }
        return vector;
    }

    /**
     * Decodes the second part of BytesRef into vector dimensions
     * executed after SparseVectorFieldMapper.decodeVector(vectorBR);
     * @param vectorBR - vector decoded in BytesRef
     * @param dimCount - number of dimensions
     */
    public static int[] decodeVectorDims(BytesRef vectorBR, int dimCount) {
        int[] dims = new int[dimCount];
        int offset =  vectorBR.offset + INT_BYTES * dimCount; //calculate the offset from where dims are encoded
        for (int dim = 0; dim < dimCount; dim++) {
            dims[dim] = ((vectorBR.bytes[offset] & 0xFF) << 8) | (vectorBR.bytes[offset+1] & 0xFF);
            offset += 2;
        }
        return dims;
    }

}
