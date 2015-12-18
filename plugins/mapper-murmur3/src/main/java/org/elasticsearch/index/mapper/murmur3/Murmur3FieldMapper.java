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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericLongAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

public class Murmur3FieldMapper extends LongFieldMapper {

    public static final String CONTENT_TYPE = "murmur3";

    public static class Defaults extends LongFieldMapper.Defaults {
        public static final MappedFieldType FIELD_TYPE = new Murmur3FieldType();
        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, Murmur3FieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Integer.MAX_VALUE);
            builder = this;
            builder.precisionStep(Integer.MAX_VALUE);
        }

        @Override
        public Murmur3FieldMapper build(BuilderContext context) {
            setupFieldType(context);
            Murmur3FieldMapper fieldMapper = new Murmur3FieldMapper(name, fieldType, defaultFieldType,
                    ignoreMalformed(context), coerce(context),
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
            return (Murmur3FieldMapper) fieldMapper.includeInAll(includeInAll);
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            if (context.indexCreatedVersion().onOrAfter(Version.V_2_0_0_beta1)) {
                fieldType.setIndexOptions(IndexOptions.NONE);
                defaultFieldType.setIndexOptions(IndexOptions.NONE);
                fieldType.setHasDocValues(true);
                defaultFieldType.setHasDocValues(true);
            }
        }

        @Override
        protected NamedAnalyzer makeNumberAnalyzer(int precisionStep) {
            return NumericLongAnalyzer.buildNamedAnalyzer(precisionStep);
        }

        @Override
        protected int maxPrecisionStep() {
            return 64;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        @SuppressWarnings("unchecked")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);

            // tweaking these settings is no longer allowed, the entire purpose of murmur3 fields is to store a hash
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_2_0_0_beta1)) {
                if (node.get("doc_values") != null) {
                    throw new MapperParsingException("Setting [doc_values] cannot be modified for field [" + name + "]");
                }
                if (node.get("index") != null) {
                    throw new MapperParsingException("Setting [index] cannot be modified for field [" + name + "]");
                }
            }

            if (parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
                builder.indexOptions(IndexOptions.DOCS);
            }

            parseNumberField(builder, name, node, parserContext);
            // Because this mapper extends LongFieldMapper the null_value field will be added to the JSON when transferring cluster state
            // between nodes so we have to remove the entry here so that the validation doesn't fail
            // TODO should murmur3 support null_value? at the moment if a user sets null_value it has to be silently ignored since we can't
            // determine whether the JSON is the original JSON from the user or if its the serialised cluster state being passed between nodes.
//            node.remove("null_value");
            return builder;
        }
    }

    // this only exists so a check can be done to match the field type to using murmur3 hashing...
    public static class Murmur3FieldType extends LongFieldMapper.LongFieldType {
        public Murmur3FieldType() {
        }

        protected Murmur3FieldType(Murmur3FieldType ref) {
            super(ref);
        }

        @Override
        public Murmur3FieldType clone() {
            return new Murmur3FieldType(this);
        }
    }

    protected Murmur3FieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
            Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
            Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, indexSettings, multiFields, copyTo);
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
            super.innerParseCreateField(context.createExternalValueContext(hash), fields);
        }

    }

    @Override
    public boolean isGenerated() {
        return true;
    }

}
