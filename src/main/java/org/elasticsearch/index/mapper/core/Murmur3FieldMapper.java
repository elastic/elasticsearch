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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.murmur3Field;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

public class Murmur3FieldMapper extends LongFieldMapper {

    public static final String CONTENT_TYPE = "murmur3";

    public static class Defaults extends LongFieldMapper.Defaults {
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, Murmur3FieldMapper> {

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE), Integer.MAX_VALUE);
            builder = this;
            builder.precisionStep(Integer.MAX_VALUE);
        }

        @Override
        public Murmur3FieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            Murmur3FieldMapper fieldMapper = new Murmur3FieldMapper(buildNames(context), fieldType.numericPrecisionStep(), boost, fieldType, docValues, ~0L,
                    ignoreMalformed(context), coerce(context), postingsProvider, docValuesProvider, similarity, normsLoading,
                    fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        @SuppressWarnings("unchecked")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = murmur3Field(name);
            parseNumberField(builder, name, node, parserContext);
            return builder;
        }
    }

    protected Murmur3FieldMapper(Names names, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
            Long nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
            PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider,
            SimilarityProvider similarity, Loading normsLoading, @Nullable Settings fieldDataSettings,
            Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues, nullValue, ignoreMalformed, coerce,
                postingsProvider, docValuesProvider, similarity, normsLoading, fieldDataSettings,
                indexSettings, multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        final Object value;
        if (context.externalValueSet()) {
            value = context.externalValue();
        } else {
            value = context.parser().textOrNull();
        }
        if (value != null) {
            final BytesRef bytes = new BytesRef(value.toString());
            final long hash = MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, new MurmurHash3.Hash128()).h1;
            context.externalValue(hash);
            super.innerParseCreateField(context, fields);
        }

    }

}
