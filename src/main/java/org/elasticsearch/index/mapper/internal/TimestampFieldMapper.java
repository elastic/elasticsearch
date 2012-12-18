/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.timestamp;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 */
public class TimestampFieldMapper extends DateFieldMapper implements InternalMapper, RootMapper {

    public static final String NAME = "_timestamp";
    public static final String CONTENT_TYPE = "_timestamp";
    public static final String DEFAULT_DATE_TIME_FORMAT = "dateOptionalTime";

    public static class Defaults extends DateFieldMapper.Defaults {
        public static final String NAME = "_timestamp";

        public static final FieldType FIELD_TYPE = new FieldType(DateFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }

        public static final boolean ENABLED = false;
        public static final String PATH = null;
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern(DEFAULT_DATE_TIME_FORMAT);
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, TimestampFieldMapper> {

        private boolean enabled = Defaults.ENABLED;
        private String path = Defaults.PATH;
        private FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE));
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return builder;
        }

        public Builder path(String path) {
            this.path = path;
            return builder;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return builder;
        }

        @Override
        public TimestampFieldMapper build(BuilderContext context) {
            boolean parseUpperInclusive = Defaults.PARSE_UPPER_INCLUSIVE;
            if (context.indexSettings() != null) {
                parseUpperInclusive = context.indexSettings().getAsBoolean("index.mapping.date.parse_upper_inclusive", Defaults.PARSE_UPPER_INCLUSIVE);
            }
            return new TimestampFieldMapper(fieldType, enabled, path, dateTimeFormatter, parseUpperInclusive,
                    ignoreMalformed(context), provider);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            TimestampFieldMapper.Builder builder = timestamp();
            parseField(builder, builder.name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("path")) {
                    builder.path(fieldNode.toString());
                } else if (fieldName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(builder.name(), fieldNode.toString()));
                }
            }
            return builder;
        }
    }


    private boolean enabled;

    private final String path;

    public TimestampFieldMapper() {
        this(new FieldType(Defaults.FIELD_TYPE), Defaults.ENABLED, Defaults.PATH, Defaults.DATE_TIME_FORMATTER,
                Defaults.PARSE_UPPER_INCLUSIVE, Defaults.IGNORE_MALFORMED, null);
    }

    protected TimestampFieldMapper(FieldType fieldType, boolean enabled, String path,
                                   FormatDateTimeFormatter dateTimeFormatter, boolean parseUpperInclusive,
                                   Explicit<Boolean> ignoreMalformed, PostingsFormatProvider provider) {
        super(new Names(Defaults.NAME, Defaults.NAME, Defaults.NAME, Defaults.NAME), dateTimeFormatter,
                Defaults.PRECISION_STEP, Defaults.FUZZY_FACTOR, Defaults.BOOST, fieldType,
                Defaults.NULL_VALUE, TimeUnit.MILLISECONDS /*always milliseconds*/,
                parseUpperInclusive, ignoreMalformed, provider, null);
        this.enabled = enabled;
        this.path = path;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    public boolean enabled() {
        return this.enabled;
    }

    public String path() {
        return this.path;
    }

    public FormatDateTimeFormatter dateTimeFormatter() {
        return this.dateTimeFormatter;
    }

    /**
     * Override the default behavior to return a timestamp
     *
     * @param value
     */
    @Override
    public Object valueForSearch(Object value) {
        return value(value);
    }

    @Override
    public String valueAsString(Object value) {
        Long val = value(value);
        if (val == null) {
            return null;
        }
        return val.toString();
    }

    @Override
    public void validate(ParseContext context) throws MapperParsingException {
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // nothing to do here, we call the parent in preParse
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected Field innerParseCreateField(ParseContext context) throws IOException {
        if (enabled) {
            long timestamp = context.sourceToParse().timestamp();
            if (!fieldType.indexed() && !fieldType.stored()) {
                context.ignoredValue(names.indexName(), String.valueOf(timestamp));
                return null;
            }
            return new LongFieldMapper.CustomLongNumericField(this, timestamp, fieldType);
        }
        return null;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // if all are defaults, no sense to write it at all
        if (fieldType.indexed() == Defaults.FIELD_TYPE.indexed() &&
                fieldType.stored() == Defaults.FIELD_TYPE.stored() && enabled == Defaults.ENABLED && path == Defaults.PATH
                && dateTimeFormatter.format().equals(Defaults.DATE_TIME_FORMATTER.format())) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (fieldType.indexed() != Defaults.FIELD_TYPE.indexed()) {
            builder.field("index", fieldType.indexed());
        }
        if (fieldType.stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", fieldType.stored());
        }
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (path != Defaults.PATH) {
            builder.field("path", path);
        }
        if (!dateTimeFormatter.format().equals(Defaults.DATE_TIME_FORMATTER.format())) {
            builder.field("format", dateTimeFormatter.format());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
