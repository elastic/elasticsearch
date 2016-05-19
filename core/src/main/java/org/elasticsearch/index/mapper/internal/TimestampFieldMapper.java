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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.LegacyDateFieldMapper;
import org.elasticsearch.index.mapper.core.LegacyLongFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.lenientNodeBooleanValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;

public class TimestampFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_timestamp";
    public static final String CONTENT_TYPE = "_timestamp";
    public static final String DEFAULT_DATE_TIME_FORMAT = "epoch_millis||strictDateOptionalTime";

    public static class Defaults extends LegacyDateFieldMapper.Defaults {
        public static final String NAME = "_timestamp";

        // TODO: this should be removed
        public static final TimestampFieldType FIELD_TYPE = new TimestampFieldType();
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern(DEFAULT_DATE_TIME_FORMAT);

        static {
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setNumericPrecisionStep(Defaults.PRECISION_STEP_64_BIT);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.setDateTimeFormatter(DATE_TIME_FORMATTER);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.freeze();
        }

        public static final EnabledAttributeMapper ENABLED = EnabledAttributeMapper.UNSET_DISABLED;
        public static final String DEFAULT_TIMESTAMP = "now";
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, TimestampFieldMapper> {

        private EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;
        private String defaultTimestamp = Defaults.DEFAULT_TIMESTAMP;
        private Boolean ignoreMissing = null;

        public Builder(MappedFieldType existing, Settings settings) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
        }

        @Override
        public LegacyDateFieldMapper.DateFieldType fieldType() {
            return (LegacyDateFieldMapper.DateFieldType)fieldType;
        }

        public Builder enabled(EnabledAttributeMapper enabledState) {
            this.enabledState = enabledState;
            return builder;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            fieldType().setDateTimeFormatter(dateTimeFormatter);
            return this;
        }

        public Builder defaultTimestamp(String defaultTimestamp) {
            this.defaultTimestamp = defaultTimestamp;
            return builder;
        }

        public Builder ignoreMissing(boolean ignoreMissing) {
            this.ignoreMissing = ignoreMissing;
            return builder;
        }

        @Override
        public Builder store(boolean store) {
            return super.store(store);
        }

        @Override
        public TimestampFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new TimestampFieldMapper(fieldType, defaultFieldType, enabledState, defaultTimestamp,
                    ignoreMissing, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME), parserContext.mapperService().getIndexSettings().getSettings());
            boolean defaultSet = false;
            Boolean ignoreMissing = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    EnabledAttributeMapper enabledState = lenientNodeBooleanValue(fieldNode) ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED;
                    builder.enabled(enabledState);
                    iterator.remove();
                } else if (fieldName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(fieldNode.toString()));
                    iterator.remove();
                } else if (fieldName.equals("default")) {
                    if (fieldNode == null) {
                        throw new TimestampParsingException("default timestamp can not be set to null");
                    } else {
                        builder.defaultTimestamp(fieldNode.toString());
                        defaultSet = true;
                    }
                    iterator.remove();
                } else if (fieldName.equals("ignore_missing")) {
                    ignoreMissing = lenientNodeBooleanValue(fieldNode);
                    builder.ignoreMissing(ignoreMissing);
                    iterator.remove();
                }
            }

            // We can not accept a default value and rejecting null values at the same time
            if (defaultSet && (ignoreMissing != null && ignoreMissing == false)) {
                throw new TimestampParsingException("default timestamp can not be set with ignore_missing set to false");
            }

            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
            return new TimestampFieldMapper(indexSettings, fieldType);
        }
    }

    public static final class TimestampFieldType extends LegacyDateFieldMapper.DateFieldType {

        public TimestampFieldType() {}

        protected TimestampFieldType(TimestampFieldType ref) {
            super(ref);
        }

        @Override
        public TimestampFieldType clone() {
            return new TimestampFieldType(this);
        }

        @Override
        public Object valueForSearch(Object value) {
            return value;
        }
    }

    private EnabledAttributeMapper enabledState;

    private final String defaultTimestamp;
    private final Boolean ignoreMissing;

    private TimestampFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing != null ? existing : Defaults.FIELD_TYPE, Defaults.FIELD_TYPE, Defaults.ENABLED, Defaults.DEFAULT_TIMESTAMP, null, indexSettings);
    }

    private TimestampFieldMapper(MappedFieldType fieldType, MappedFieldType defaultFieldType, EnabledAttributeMapper enabledState,
                                   String defaultTimestamp, Boolean ignoreMissing, Settings indexSettings) {
        super(NAME, fieldType, defaultFieldType, indexSettings);
        this.enabledState = enabledState;
        this.defaultTimestamp = defaultTimestamp;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public TimestampFieldType fieldType() {
        return (TimestampFieldType)super.fieldType();
    }

    public boolean enabled() {
        return this.enabledState.enabled;
    }

    public String defaultTimestamp() {
        return this.defaultTimestamp;
    }

    public Boolean ignoreMissing() {
        return this.ignoreMissing;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we call the parent in preParse
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (enabledState.enabled) {
            long timestamp = context.sourceToParse().timestamp();
            if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
                fields.add(new LegacyLongFieldMapper.CustomLongNumericField(timestamp, fieldType()));
            }
            if (fieldType().hasDocValues()) {
                fields.add(new NumericDocValuesField(fieldType().name(), timestamp));
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // if all are defaults, no sense to write it at all
        if (!includeDefaults && enabledState == Defaults.ENABLED
                && fieldType().dateTimeFormatter().format().equals(Defaults.DATE_TIME_FORMATTER.format())
                && Defaults.DEFAULT_TIMESTAMP.equals(defaultTimestamp)) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (includeDefaults || enabledState != Defaults.ENABLED) {
            builder.field("enabled", enabledState.enabled);
        }
        // different format handling depending on index version
        String defaultDateFormat = Defaults.DATE_TIME_FORMATTER.format();
        if (includeDefaults || !fieldType().dateTimeFormatter().format().equals(defaultDateFormat)) {
            builder.field("format", fieldType().dateTimeFormatter().format());
        }
        if (includeDefaults || !Defaults.DEFAULT_TIMESTAMP.equals(defaultTimestamp)) {
            builder.field("default", defaultTimestamp);
        }
        if (includeDefaults || ignoreMissing != null) {
            builder.field("ignore_missing", ignoreMissing);
        }

        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        TimestampFieldMapper timestampFieldMapperMergeWith = (TimestampFieldMapper) mergeWith;
        super.doMerge(mergeWith, updateAllTypes);
        if (timestampFieldMapperMergeWith.enabledState != enabledState && !timestampFieldMapperMergeWith.enabledState.unset()) {
            this.enabledState = timestampFieldMapperMergeWith.enabledState;
        }
        if (timestampFieldMapperMergeWith.defaultTimestamp() == null && defaultTimestamp == null) {
            return;
        }
        List<String> conflicts = new ArrayList<>();
        if (defaultTimestamp == null) {
            conflicts.add("Cannot update default in _timestamp value. Value is null now encountering " + timestampFieldMapperMergeWith.defaultTimestamp());
        } else if (timestampFieldMapperMergeWith.defaultTimestamp() == null) {
            conflicts.add("Cannot update default in _timestamp value. Value is \" + defaultTimestamp.toString() + \" now encountering null");
        } else if (!timestampFieldMapperMergeWith.defaultTimestamp().equals(defaultTimestamp)) {
            conflicts.add("Cannot update default in _timestamp value. Value is " + defaultTimestamp.toString() + " now encountering " + timestampFieldMapperMergeWith.defaultTimestamp());
        }
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("Conflicts: " + conflicts);
        }
    }
}
