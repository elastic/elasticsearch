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
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */

public class DenseVectorFieldMapper extends FieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "dense_vector";
    private static final int INT_BYTES = Integer.BYTES;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new DenseVectorFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, DenseVectorFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public DenseVectorFieldType fieldType() {
            return (DenseVectorFieldType) super.fieldType();
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new DenseVectorFieldMapper(
                    name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DenseVectorFieldMapper.Builder builder = new DenseVectorFieldMapper.Builder(name);
            return builder;
        }
    }

    public static final class DenseVectorFieldType extends MappedFieldType {

        public DenseVectorFieldType() {}

        protected DenseVectorFieldType(DenseVectorFieldType ref) {
            super(ref);
        }

        public DenseVectorFieldType clone() {
            return new DenseVectorFieldType(this);
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
            throw new UnsupportedOperationException("[dense_vector] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("Queries on [dense_vector] fields are not supported");
        }
    }


    private DenseVectorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                   Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
    }

    @Override
    protected DenseVectorFieldMapper clone() {
        return (DenseVectorFieldMapper) super.clone();
    }

    @Override
    public DenseVectorFieldType fieldType() {
        return (DenseVectorFieldType) super.fieldType();
    }

    @Override
    public FieldMapper parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("[dense_vector] field can't be used in multi-fields");
        }
        // buffer will encode array of floats
        // buffer will first contain an integer -  number of dimensions
        // after that arrays elements encoded as integers

        byte[] buf = new byte[INT_BYTES + 10 * INT_BYTES]; // initially allocating buffer for 10 dimensions
        int offset = INT_BYTES;
        int dim = 0;
        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
            if (token == Token.VALUE_NUMBER) {
                float value = context.parser().floatValue(true);
                if (buf.length < (offset + INT_BYTES)) {
                    buf = ArrayUtil.grow(buf, (offset + INT_BYTES));
                }
                NumericUtils.intToSortableBytes(Float.floatToIntBits(value), buf, offset);
                offset = offset + INT_BYTES;
                dim++;
            } else {
                throw new IllegalArgumentException("[dense_vector] field take an array of floats, but got unexpected token " + token);
            }
        }
        NumericUtils.intToSortableBytes(dim, buf, 0); //recording number of dimensions at the beginning
        BinaryDocValuesField field = new BinaryDocValuesField(fieldType().name(), new BytesRef(buf, 0, offset));
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


    //**************STATIC HELPER METHODS***********************************
    // Decodes a BytesRef into a dense array <code>vector</code>
    // TODO: possibly have another type of DocValuesField where an array of floats can be already decoded
    public static float[] decodeVector(BytesRef vectorBR) {
        int dimCount = NumericUtils.sortableBytesToInt(vectorBR.bytes, vectorBR.offset);
        float[] vector = new float[dimCount];
        int offset =  vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            offset = offset + INT_BYTES;
            vector[dim] = Float.intBitsToFloat(NumericUtils.sortableBytesToInt(vectorBR.bytes, offset));
        }
        return vector;
    }
}
