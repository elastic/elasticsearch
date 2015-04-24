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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.timestamp;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

public class TimestampFieldMapper extends DateFieldMapper implements InternalMapper, RootMapper {

    public static final String NAME = "_timestamp";
    public static final String CONTENT_TYPE = "_timestamp";
    public static final String DEFAULT_DATE_TIME_FORMAT = "dateOptionalTime";

    public static class Defaults extends DateFieldMapper.Defaults {
        public static final String NAME = "_timestamp";

        // TODO: this should be removed
        public static final FieldType PRE_20_FIELD_TYPE;
        public static final FieldType FIELD_TYPE = new FieldType(DateFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
            PRE_20_FIELD_TYPE = new FieldType(FIELD_TYPE);
            PRE_20_FIELD_TYPE.setStored(false);
            PRE_20_FIELD_TYPE.freeze();
        }

        public static final EnabledAttributeMapper ENABLED = EnabledAttributeMapper.UNSET_DISABLED;
        public static final String PATH = null;
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern(DEFAULT_DATE_TIME_FORMAT);
        public static final String DEFAULT_TIMESTAMP = "now";
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, TimestampFieldMapper> {

        private EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;
        private String path = Defaults.PATH;
        private FormatDateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;
        private String defaultTimestamp = Defaults.DEFAULT_TIMESTAMP;
        private boolean explicitStore = false;
        private Boolean ignoreMissing = null;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE), Defaults.PRECISION_STEP_64_BIT);
        }

        public Builder enabled(EnabledAttributeMapper enabledState) {
            this.enabledState = enabledState;
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
            explicitStore = true;
            return super.store(store);
        }

        @Override
        public TimestampFieldMapper build(BuilderContext context) {
            if (explicitStore == false && context.indexCreatedVersion().before(Version.V_2_0_0)) {
                assert fieldType.stored();
                fieldType.setStored(false);
            }
            return new TimestampFieldMapper(fieldType, docValues, enabledState, path, dateTimeFormatter, defaultTimestamp,
                    ignoreMissing,
                    ignoreMalformed(context), coerce(context), normsLoading, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            TimestampFieldMapper.Builder builder = timestamp();
            parseField(builder, builder.name, node, parserContext);
            boolean defaultSet = false;
            Boolean ignoreMissing = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    EnabledAttributeMapper enabledState = nodeBooleanValue(fieldNode) ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED;
                    builder.enabled(enabledState);
                    iterator.remove();
                } else if (fieldName.equals("path")) {
                    builder.path(fieldNode.toString());
                    iterator.remove();
                } else if (fieldName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(fieldNode.toString()));
                    iterator.remove();
                } else if (fieldName.equals("default")) {
                    if (fieldNode == null) {
                        if (parserContext.indexVersionCreated().onOrAfter(Version.V_1_4_0_Beta1) &&
                                parserContext.indexVersionCreated().before(Version.V_1_5_0)) {
                            // We are reading an index created in 1.4 with feature #7036
                            // `default: null` was explicitly set. We need to change this index to
                            // `ignore_missing: false`
                            builder.ignoreMissing(false);
                        } else {
                            throw new TimestampParsingException("default timestamp can not be set to null");
                        }
                    } else {
                        builder.defaultTimestamp(fieldNode.toString());
                        defaultSet = true;
                    }
                    iterator.remove();
                } else if (fieldName.equals("ignore_missing")) {
                    ignoreMissing = nodeBooleanValue(fieldNode);
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
    }

    private static FieldType defaultFieldType(Settings settings) {
        return Version.indexCreated(settings).onOrAfter(Version.V_2_0_0) ? Defaults.FIELD_TYPE : Defaults.PRE_20_FIELD_TYPE;
    }

    private EnabledAttributeMapper enabledState;

    private final String path;
    private final String defaultTimestamp;
    private final FieldType defaultFieldType;
    private final Boolean ignoreMissing;

    public TimestampFieldMapper(Settings indexSettings) {
        this(new FieldType(defaultFieldType(indexSettings)), null, Defaults.ENABLED, Defaults.PATH, Defaults.DATE_TIME_FORMATTER, Defaults.DEFAULT_TIMESTAMP,
             null, Defaults.IGNORE_MALFORMED, Defaults.COERCE, null, null, indexSettings);
    }

    protected TimestampFieldMapper(FieldType fieldType, Boolean docValues, EnabledAttributeMapper enabledState, String path,
                                   FormatDateTimeFormatter dateTimeFormatter, String defaultTimestamp,
                                   Boolean ignoreMissing,
                                   Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce, Loading normsLoading,
                                   @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(Defaults.NAME, Defaults.NAME, Defaults.NAME, Defaults.NAME), dateTimeFormatter,
                Defaults.PRECISION_STEP_64_BIT, Defaults.BOOST, fieldType, docValues,
                Defaults.NULL_VALUE, TimeUnit.MILLISECONDS /*always milliseconds*/,
                ignoreMalformed, coerce, null, normsLoading, fieldDataSettings, 
                indexSettings, MultiFields.empty(), null);
        this.enabledState = enabledState;
        this.path = path;
        this.defaultTimestamp = defaultTimestamp;
        this.defaultFieldType = defaultFieldType(indexSettings);
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public FieldType defaultFieldType() {
        return defaultFieldType;
    }

    @Override
    public boolean defaultDocValues() {
        return false;
    }

    public boolean enabled() {
        return this.enabledState.enabled;
    }

    public String path() {
        return this.path;
    }

    public String defaultTimestamp() {
        return this.defaultTimestamp;
    }

    public Boolean ignoreMissing() {
        return this.ignoreMissing;
    }

    @Override
    public FormatDateTimeFormatter dateTimeFormatter() {
        return this.dateTimeFormatter;
    }

    /**
     * Override the default behavior to return a timestamp
     */
    @Override
    public Object valueForSearch(Object value) {
        return value(value);
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
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (enabledState.enabled) {
            long timestamp = context.sourceToParse().timestamp();
            if (fieldType.indexOptions() == IndexOptions.NONE && !fieldType.stored() && !hasDocValues()) {
                context.ignoredValue(names.indexName(), String.valueOf(timestamp));
            }
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                fields.add(new LongFieldMapper.CustomLongNumericField(this, timestamp, fieldType));
            }
            if (hasDocValues()) {
                fields.add(new NumericDocValuesField(names.indexName(), timestamp));
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
        boolean indexed = fieldType.indexOptions() != IndexOptions.NONE;
        boolean indexedDefault = Defaults.FIELD_TYPE.indexOptions() != IndexOptions.NONE;

        // if all are defaults, no sense to write it at all
        if (!includeDefaults && indexed == indexedDefault && customFieldDataSettings == null &&
                fieldType.stored() == Defaults.FIELD_TYPE.stored() && enabledState == Defaults.ENABLED && path == Defaults.PATH
                && dateTimeFormatter.format().equals(Defaults.DATE_TIME_FORMATTER.format())
                && Defaults.DEFAULT_TIMESTAMP.equals(defaultTimestamp)
                && defaultDocValues() == hasDocValues()) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (includeDefaults || enabledState != Defaults.ENABLED) {
            builder.field("enabled", enabledState.enabled);
        }
        if (includeDefaults || (indexed != indexedDefault) || (fieldType.tokenized() != Defaults.FIELD_TYPE.tokenized())) {
            builder.field("index", indexTokenizeOptionToString(indexed, fieldType.tokenized()));
        }
        if (includeDefaults || fieldType.stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", fieldType.stored());
        }
        doXContentDocValues(builder, includeDefaults);
        if (includeDefaults || path != Defaults.PATH) {
            builder.field("path", path);
        }
        if (includeDefaults || !dateTimeFormatter.format().equals(Defaults.DATE_TIME_FORMATTER.format())) {
            builder.field("format", dateTimeFormatter.format());
        }
        if (includeDefaults || !Defaults.DEFAULT_TIMESTAMP.equals(defaultTimestamp)) {
            builder.field("default", defaultTimestamp);
        }
        if (includeDefaults || ignoreMissing != null) {
            builder.field("ignore_missing", ignoreMissing);
        }
        if (customFieldDataSettings != null) {
            builder.field("fielddata", (Map) customFieldDataSettings.getAsMap());
        } else if (includeDefaults) {
            builder.field("fielddata", (Map) fieldDataType.getSettings().getAsMap());
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        TimestampFieldMapper timestampFieldMapperMergeWith = (TimestampFieldMapper) mergeWith;
        super.merge(mergeWith, mergeResult);
        if (!mergeResult.simulate()) {
            if (timestampFieldMapperMergeWith.enabledState != enabledState && !timestampFieldMapperMergeWith.enabledState.unset()) {
                this.enabledState = timestampFieldMapperMergeWith.enabledState;
            }
        } else {
            if (timestampFieldMapperMergeWith.defaultTimestamp() == null && defaultTimestamp == null) {
                return;
            }
            if (defaultTimestamp == null) {
                mergeResult.addConflict("Cannot update default in _timestamp value. Value is null now encountering " + timestampFieldMapperMergeWith.defaultTimestamp());
            } else if (timestampFieldMapperMergeWith.defaultTimestamp() == null) {
                mergeResult.addConflict("Cannot update default in _timestamp value. Value is \" + defaultTimestamp.toString() + \" now encountering null");
            } else if (!timestampFieldMapperMergeWith.defaultTimestamp().equals(defaultTimestamp)) {
                mergeResult.addConflict("Cannot update default in _timestamp value. Value is " + defaultTimestamp.toString() + " now encountering " + timestampFieldMapperMergeWith.defaultTimestamp());
            }
            if (this.path != null) {
                if (path.equals(timestampFieldMapperMergeWith.path()) == false) {
                    mergeResult.addConflict("Cannot update path in _timestamp value. Value is " + path + " path in merged mapping is " + (timestampFieldMapperMergeWith.path() == null ? "missing" : timestampFieldMapperMergeWith.path()));
                }
            } else if (timestampFieldMapperMergeWith.path() != null) {
                mergeResult.addConflict("Cannot update path in _timestamp value. Value is " + path + " path in merged mapping is missing");
            }
        }
    }
}
