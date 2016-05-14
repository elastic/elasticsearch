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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Field;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.StringFieldMapper.ValueAndBoost;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.index.IndexOptions.NONE;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 * A {@link FieldMapper} that takes a string and writes a count of the tokens in that string
 * to the index.  In most ways the mapper acts just like an {@link LegacyIntegerFieldMapper}.
 */
public class LegacyTokenCountFieldMapper extends LegacyIntegerFieldMapper {
    public static final String CONTENT_TYPE = "token_count";

    public static class Defaults extends LegacyIntegerFieldMapper.Defaults {

    }

    public static class Builder extends LegacyNumberFieldMapper.Builder<Builder, LegacyTokenCountFieldMapper> {
        private NamedAnalyzer analyzer;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_32_BIT);
            builder = this;
        }

        public Builder analyzer(NamedAnalyzer analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        public NamedAnalyzer analyzer() {
            return analyzer;
        }

        @Override
        public LegacyTokenCountFieldMapper build(BuilderContext context) {
            if (context.indexCreatedVersion().onOrAfter(Version.V_5_0_0_alpha2)) {
                throw new IllegalStateException("Cannot use legacy numeric types after 5.0");
            }
            setupFieldType(context);
            LegacyTokenCountFieldMapper fieldMapper = new LegacyTokenCountFieldMapper(name, fieldType, defaultFieldType,
                    ignoreMalformed(context), coerce(context), context.indexSettings(),
                    analyzer, multiFieldsBuilder.build(this, context), copyTo);
            return (LegacyTokenCountFieldMapper) fieldMapper.includeInAll(includeInAll);
        }

        @Override
        protected int maxPrecisionStep() {
            return 32;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        @SuppressWarnings("unchecked")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            LegacyTokenCountFieldMapper.Builder builder = new LegacyTokenCountFieldMapper.Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    builder.nullValue(nodeIntegerValue(propNode));
                    iterator.remove();
                } else if (propName.equals("analyzer")) {
                    NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                    }
                    builder.analyzer(analyzer);
                    iterator.remove();
                }
            }
            parseNumberField(builder, name, node, parserContext);
            if (builder.analyzer() == null) {
                throw new MapperParsingException("Analyzer must be set for field [" + name + "] but wasn't.");
            }
            return builder;
        }
    }

    private NamedAnalyzer analyzer;

    protected LegacyTokenCountFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Explicit<Boolean> ignoreMalformed,
                                    Explicit<Boolean> coerce, Settings indexSettings, NamedAnalyzer analyzer, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, indexSettings, multiFields, copyTo);
        this.analyzer = analyzer;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        ValueAndBoost valueAndBoost = StringFieldMapper.parseCreateFieldForString(context, null /* Out null value is an int so we convert*/, fieldType().boost());
        if (valueAndBoost.value() == null && fieldType().nullValue() == null) {
            return;
        }

        if (fieldType().indexOptions() != NONE || fieldType().stored() || fieldType().hasDocValues()) {
            int count;
            if (valueAndBoost.value() == null) {
                count = fieldType().nullValue();
            } else {
                count = countPositions(analyzer, simpleName(), valueAndBoost.value());
            }
            addIntegerFields(context, fields, count, valueAndBoost.boost());
        }
    }

    /**
     * Count position increments in a token stream.  Package private for testing.
     * @param analyzer analyzer to create token stream
     * @param fieldName field name to pass to analyzer
     * @param fieldValue field value to pass to analyzer
     * @return number of position increments in a token stream
     * @throws IOException if tokenStream throws it
     */
    static int countPositions(Analyzer analyzer, String fieldName, String fieldValue) throws IOException {
        try (TokenStream tokenStream = analyzer.tokenStream(fieldName, fieldValue)) {
            int count = 0;
            PositionIncrementAttribute position = tokenStream.addAttribute(PositionIncrementAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                count += position.getPositionIncrement();
            }
            tokenStream.end();
            count += position.getPositionIncrement();
            return count;
        }
    }

    /**
     * Name of analyzer.
     * @return name of analyzer
     */
    public String analyzer() {
        return analyzer.name();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        this.analyzer = ((LegacyTokenCountFieldMapper) mergeWith).analyzer;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        builder.field("analyzer", analyzer());
    }

    @Override
    public boolean isGenerated() {
        return true;
    }

}
