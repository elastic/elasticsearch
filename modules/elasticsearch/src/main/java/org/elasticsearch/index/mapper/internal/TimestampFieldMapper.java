/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import java.io.IOException;
import java.util.Date;

/**
 * @author paikan (benjamin.deveze)
 */
public class TimestampFieldMapper extends DateFieldMapper implements InternalMapper {

    public static final String CONTENT_TYPE = "_timestamp";

    public static class Defaults extends DateFieldMapper.Defaults {
        public static final boolean ENABLED = false;
        public static final String NAME = CONTENT_TYPE;
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final Field.Store STORE = Field.Store.NO;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, TimestampFieldMapper> {

        protected boolean enabled = Defaults.ENABLED;

        protected FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;

        public Builder() {
            super(Defaults.NAME);
            index = Defaults.INDEX;
            store = Defaults.STORE;
            omitNorms = Defaults.OMIT_NORMS;
            omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
            builder = this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return builder;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        @Override public TimestampFieldMapper build(BuilderContext context) {
            TimestampFieldMapper fieldMapper = new TimestampFieldMapper(enabled, dateTimeFormatter, buildNames(context),
                    precisionStep, fuzzyFactor, index, store, boost, omitNorms, omitTermFreqAndPositions);
            fieldMapper.includeInAll(includeInAll);
            return fieldMapper;
        }
    }

    private final boolean enabled;

    private final FormatDateTimeFormatter dateTimeFormatter;

    public TimestampFieldMapper() {
        this(Defaults.NAME);
    }

    public TimestampFieldMapper(String name) {
        this(Defaults.ENABLED, Defaults.DATE_TIME_FORMATTER, new Names(name), Defaults.PRECISION_STEP,
                Defaults.FUZZY_FACTOR, Defaults.INDEX, Defaults.STORE, Defaults.BOOST, Defaults.OMIT_NORMS,
                Defaults.OMIT_TERM_FREQ_AND_POSITIONS);
    }

    public TimestampFieldMapper(boolean enabled, FormatDateTimeFormatter dateTimeFormatter, Names names,
                                int precisionStep, String fuzzyFactor, Field.Index index, Field.Store store,
                                float boost, boolean omitNorms, boolean omitTermFreqAndPositions) {
        super(names, dateTimeFormatter, precisionStep, fuzzyFactor, index, store, boost, omitNorms, omitTermFreqAndPositions, null);
        this.enabled = enabled;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override protected String contentType() {
        return Defaults.NAME;
    }

    public boolean enabled() {
        return this.enabled;
    }

    public FormatDateTimeFormatter dateTimeFormatter() {
        return this.dateTimeFormatter;
    }

    /**
     * Override the default behavior (to return the string, and return the actual Number instance).
     */
    @Override public Object valueForSearch(Fieldable field) {
        return value(field);
    }

    @Override public String valueAsString(Fieldable field) {
        Long value = value(field);
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override protected Fieldable parseCreateField(ParseContext context) throws IOException {
        if (!enabled) {
            return null;
        }

        if (context.parsedTimestampState() == ParseContext.ParsedTimestampState.NO) {
            String dateAsString = null;
            Long value = null;
            float boost = this.boost;
            XContentParser parser = context.parser();
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                // In case of null value, the _timestamp will be generated
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.longValue();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("value".equals(currentFieldName) || "_value".equals(currentFieldName)) {
                            if (token == XContentParser.Token.VALUE_NULL) {
                                // In case of null value, the _timestamp will be generated
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                value = parser.longValue();
                            } else {
                                dateAsString = parser.text();
                            }
                        } else if ("boost".equals(currentFieldName) || "_boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        }
                    }
                }
            } else {
                dateAsString = parser.text();
            }

            if (value == null && dateAsString == null) {
                return null;
            }

            if (value == null) {
                try {
                value = parseStringValue(dateAsString);
                } catch (Exception e) {
                    return null;
                }
            }
            System.out.println("Setting timestamp to: " + value + " [" + new Date(value).toString() + "]");
            context.parsedTimestamp(ParseContext.ParsedTimestampState.PARSED);
            LongFieldMapper.CustomLongNumericField field = new LongFieldMapper.CustomLongNumericField(this, value);
            field.setBoost(boost);
            return field;
        } else if (context.parsedTimestampState() == ParseContext.ParsedTimestampState.GENERATED) {
            long value = ((Number) context.externalValue()).longValue();
            System.out.println("Setting timestamp to: " + value + " [" + new Date(value).toString() + "]");
            return new LongFieldMapper.CustomLongNumericField(this, value);
        } else {
            throw new MapperParsingException("Illegal parsed timestamp state");
        }
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // all are defaults, don't write it at all
        if (enabled == Defaults.ENABLED
                && store == Defaults.STORE
                && dateTimeFormatter.format() == Defaults.DATE_TIME_FORMATTER.format()) {
            return builder;
        }
        builder.startObject(contentType());
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (store != Defaults.STORE) {
            builder.field("store", store.name().toLowerCase());
        }
        if (dateTimeFormatter.format() != Defaults.DATE_TIME_FORMATTER.format()) {
            builder.field("format", dateTimeFormatter().format());
        }
        builder.endObject();
        return builder;
    }

    @Override public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // maybe allow to change enabled? But then we need to figure out null for default value
    }
}