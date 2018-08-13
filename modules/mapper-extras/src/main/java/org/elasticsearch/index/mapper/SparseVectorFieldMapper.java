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
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A {@link FieldMapper} for indexing a sparse vector of floats.
 */

public class SparseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "sparse_vector";
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
            return DocValueFormat.BINARY;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            throw new UnsupportedOperationException("[sparse_vector] fields do not support sorting, scripting or aggregating");
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
    public FieldMapper parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("[sparse_vector] field can't be used in multi-fields");
        }
        if (context.parser().currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException("[sparse_vector] fields must be json objects, expected a START_OBJECT but got: " +
                context.parser().currentToken());
        }
        int[] dims = new int[10]; //initialize with default of 10 dims
        float[] values = new float[10]; //initialize with default of 10 dims
        int dimCount = 0;
        int dim = 0;
        for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
            if (token == Token.FIELD_NAME) {
                dim = Integer.parseInt(context.parser().currentName());
            } else if (token == Token.VALUE_NUMBER) {
                float value = context.parser().floatValue(true);
                if (dims.length <= dimCount) { // ensure arrays have enough capacity
                    ArrayUtil.grow(values, dimCount + 1);
                    ArrayUtil.grow(dims, dimCount + 1);
                }
                sortedInsertDimValue(values, dims, value, dim, dimCount);
                dimCount ++;
            } else {
                throw new IllegalArgumentException("[sparse_vector] field takes an object that maps a dimension number to a float, " +
                    "but got unexpected token " + token);
            }
        }

        BytesRef br = encodeSparseVector(values, dims, dimCount);
        BinaryDocValuesField field = new BinaryDocValuesField(fieldType().name(), br);
        context.doc().addWithKey(fieldType().name(), field);
        return null; // no mapping update
    }


    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
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
     * BytesRef --> int DimCount, int[] floats encoded as integers values, int[] dims
     * @param values - values of the sparse array
     * @param dims - dims of the sparse array
     * @param dimCount - number of the dimension
     * @return BytesRef
     */
    private static BytesRef encodeSparseVector(float[] values, int[] dims, int dimCount) {
        byte[] buf = new byte[INT_BYTES + dimCount * INT_BYTES * 2];
        NumericUtils.intToSortableBytes(dimCount, buf, 0);
        int offset = INT_BYTES;
        for (int dim = 0; dim < dimCount; dim++) {
            NumericUtils.intToSortableBytes(Float.floatToIntBits(values[dim]), buf, offset);
            offset += INT_BYTES;
        }
        for (int dim = 0; dim < dimCount; dim++) {
            NumericUtils.intToSortableBytes(dims[dim], buf, offset);
            offset += INT_BYTES;
        }
        return new BytesRef(buf);
    }

    /**
     * Decodes a BytesRef into vector dimensions
     * executed after DenseVectorFieldMapper.decodeVector(vectorBR);
     * @param vectorBR
     * @param dimCount
     */
    public static int[] decodeVectorDims(BytesRef vectorBR, int dimCount) {
        int[] dims = new int[dimCount];
        int offset =  vectorBR.offset + INT_BYTES * dimCount; //calculate the offset from where dims are encoded
        for (int dim = 0; dim < dimCount; dim++) {
            offset = offset + INT_BYTES;
            dims[dim] = NumericUtils.sortableBytesToInt(vectorBR.bytes, offset);
        }
        return dims;
    }

}
