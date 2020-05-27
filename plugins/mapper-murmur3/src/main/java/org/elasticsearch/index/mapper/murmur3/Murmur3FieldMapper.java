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

package org.elasticsearch.index.mapper.murmur3;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Murmur3FieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "murmur3";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new Murmur3FieldType();
        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public Murmur3FieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new Murmur3FieldMapper(name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType.setIndexOptions(IndexOptions.NONE);
            defaultFieldType.setIndexOptions(IndexOptions.NONE);
            fieldType.setHasDocValues(true);
            defaultFieldType.setHasDocValues(true);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new Builder(name);

            // tweaking these settings is no longer allowed, the entire purpose of murmur3 fields is to store a hash
            if (node.get("doc_values") != null) {
                throw new MapperParsingException("Setting [doc_values] cannot be modified for field [" + name + "]");
            }
            if (node.get("index") != null) {
                throw new MapperParsingException("Setting [index] cannot be modified for field [" + name + "]");
            }

            TypeParsers.parseField(builder, name, node, parserContext);

            return builder;
        }
    }

    // this only exists so a check can be done to match the field type to using murmur3 hashing...
    public static class Murmur3FieldType extends MappedFieldType {
        public Murmur3FieldType() {
        }

        protected Murmur3FieldType(Murmur3FieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Murmur3FieldType clone() {
            return new Murmur3FieldType(this);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(NumericType.LONG);
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return CoreValuesSourceType.NUMERIC;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Murmur3 fields are not searchable: [" + name() + "]");
        }
    }

    protected Murmur3FieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
            Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context)
            throws IOException {
        final Object value;
        if (context.externalValueSet()) {
            value = context.externalValue();
        } else {
            value = context.parser().textOrNull();
        }
        if (value != null) {
            final BytesRef bytes = new BytesRef(value.toString());
            final long hash = MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, new MurmurHash3.Hash128()).h1;
            context.doc().add(new SortedNumericDocValuesField(fieldType().name(), hash));
            if (fieldType().stored()) {
                context.doc().add(new StoredField(name(), hash));
            }
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

}
