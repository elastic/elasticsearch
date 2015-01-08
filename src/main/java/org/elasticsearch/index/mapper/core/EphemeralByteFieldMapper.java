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

import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeByteValue;
import static org.elasticsearch.index.mapper.MapperBuilders.ephemeralByteField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class EphemeralByteFieldMapper extends ByteFieldMapper {

    public static final String CONTENT_TYPE = "ephemeral_byte";

    public static class Builder extends NumberFieldMapper.Builder<Builder, EphemeralByteFieldMapper> {

        protected Byte nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE), Defaults.PRECISION_STEP_8_BIT);
            builder = this;
        }

        public Builder nullValue(byte nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override
        public EphemeralByteFieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            EphemeralByteFieldMapper fieldMapper = new EphemeralByteFieldMapper(buildNames(context),
                    fieldType.numericPrecisionStep(), boost, fieldType, docValues, nullValue, ignoreMalformed(context),
                    coerce(context), postingsProvider, docValuesProvider, similarity, normsLoading, 
                    fieldDataSettings, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            EphemeralByteFieldMapper.Builder builder = ephemeralByteField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(nodeByteValue(propNode));
                }
            }
            return builder;
        }
    }

    protected EphemeralByteFieldMapper(Names names, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
                              Byte nullValue, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce, 
                              PostingsFormatProvider postingsProvider,
                              DocValuesFormatProvider docValuesProvider, SimilarityProvider similarity, Loading normsLoading,
                              @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, precisionStep, boost, fieldType, docValues,
                nullValue, ignoreMalformed, coerce,
                postingsProvider,
                docValuesProvider, similarity, normsLoading,
                fieldDataSettings, indexSettings, multiFields, copyTo);
    }


    @Override
    protected String contentType() { return CONTENT_TYPE; }

    @Override
    public boolean isEphemeral() { return true; }
}
