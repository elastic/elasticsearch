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
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.mapper.MapperBuilders.ephemeralDateField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseNumberField;

/**
 *
 */
public class EphemeralDateFieldMapper extends DateFieldMapper {

    public static final String CONTENT_TYPE = "ephemeral_date";

    public static class Builder extends NumberFieldMapper.Builder<Builder, EphemeralDateFieldMapper> {

        protected TimeUnit timeUnit = Defaults.TIME_UNIT;

        protected String nullValue = Defaults.NULL_VALUE;

        protected FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;

        private Locale locale;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE), Defaults.PRECISION_STEP_64_BIT);
            builder = this;
            // do *NOT* rely on the default locale
            locale = Locale.ROOT;
        }

        public Builder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        @Override
        public EphemeralDateFieldMapper build(BuilderContext context) {
            fieldType.setOmitNorms(fieldType.omitNorms() && boost == 1.0f);
            if (!locale.equals(dateTimeFormatter.locale())) {
                dateTimeFormatter = new FormatDateTimeFormatter(dateTimeFormatter.format(), dateTimeFormatter.parser(), dateTimeFormatter.printer(), locale);
            }
            EphemeralDateFieldMapper fieldMapper = new EphemeralDateFieldMapper(buildNames(context), dateTimeFormatter,
                    fieldType.numericPrecisionStep(), boost, fieldType, docValues, nullValue, timeUnit, ignoreMalformed(context), coerce(context),
                    postingsProvider, docValuesProvider, similarity, normsLoading, fieldDataSettings, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }

        public Builder locale(Locale locale) {
            this.locale = locale;
            return this;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            EphemeralDateFieldMapper.Builder builder = ephemeralDateField(name);
            parseNumberField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                } else if (propName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(propNode));
                } else if (propName.equals("numeric_resolution")) {
                    builder.timeUnit(TimeUnit.valueOf(propNode.toString().toUpperCase(Locale.ROOT)));
                } else if (propName.equals("locale")) {
                    builder.locale(LocaleUtils.parse(propNode.toString()));
                }
            }
            return builder;
        }
    }

    protected EphemeralDateFieldMapper(Names names, FormatDateTimeFormatter dateTimeFormatter, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
            String nullValue, TimeUnit timeUnit, Explicit<Boolean> ignoreMalformed,Explicit<Boolean> coerce,
            PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider, SimilarityProvider similarity,
            Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(names, dateTimeFormatter, precisionStep, boost, fieldType, docValues,
                nullValue, timeUnit, ignoreMalformed, coerce,
                postingsProvider, docValuesProvider, similarity,
                normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
    }

    @Override
    protected String contentType() { return CONTENT_TYPE; }

    @Override
    public boolean isEphemeral() { return true; }
}
