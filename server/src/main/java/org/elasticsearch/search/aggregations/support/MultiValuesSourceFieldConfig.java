/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

public class MultiValuesSourceFieldConfig implements Writeable, ToXContentObject {
    private final String fieldName;
    private final Object missing;
    private final Script script;
    // supported only if timezoneAware == true
    private final ZoneId timeZone;
    // supported only if filtered == true
    private final QueryBuilder filter;
    // supported only if heterogeneous == true
    private final ValueType userValueTypeHint;
    private final String format;

    private static final String NAME = "field_config";

    public static final ParseField FILTER = new ParseField("filter");

    /**
     * Creates a parser capable of parsing value sources in different context
     * @param scriptable - allows specifying script in addition to a field as a values source
     * @param timezoneAware - allows specifying timezone
     * @param filtered - allows specifying filters on the values
     * @param heterogeneous - allows specifying value-source specific format and user value type hint
     * @param <C> - parser context
     * @return configured parser
     */
    public static <C> ObjectParser<MultiValuesSourceFieldConfig.Builder, C> parserBuilder(
        boolean scriptable,
        boolean timezoneAware,
        boolean filtered,
        boolean heterogeneous
    ) {

        ObjectParser<MultiValuesSourceFieldConfig.Builder, C> parser = new ObjectParser<>(
            MultiValuesSourceFieldConfig.NAME,
            MultiValuesSourceFieldConfig.Builder::new
        );

        parser.declareString(MultiValuesSourceFieldConfig.Builder::setFieldName, ParseField.CommonFields.FIELD);
        parser.declareField(
            MultiValuesSourceFieldConfig.Builder::setMissing,
            XContentParser::objectText,
            ParseField.CommonFields.MISSING,
            ObjectParser.ValueType.VALUE
        );

        if (scriptable) {
            parser.declareField(
                MultiValuesSourceFieldConfig.Builder::setScript,
                (p, context) -> Script.parse(p),
                Script.SCRIPT_PARSE_FIELD,
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
        }

        if (timezoneAware) {
            parser.declareField(MultiValuesSourceFieldConfig.Builder::setTimeZone, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return ZoneId.of(p.text());
                } else {
                    return ZoneOffset.ofHours(p.intValue());
                }
            }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
        }

        if (filtered) {
            parser.declareField(
                MultiValuesSourceFieldConfig.Builder::setFilter,
                (p, context) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
                FILTER,
                ObjectParser.ValueType.OBJECT
            );
        }

        if (heterogeneous) {
            parser.declareField(
                MultiValuesSourceFieldConfig.Builder::setUserValueTypeHint,
                p -> ValueType.lenientParse(p.text()),
                ValueType.VALUE_TYPE,
                ObjectParser.ValueType.STRING
            );

            parser.declareField(
                MultiValuesSourceFieldConfig.Builder::setFormat,
                XContentParser::text,
                ParseField.CommonFields.FORMAT,
                ObjectParser.ValueType.STRING
            );
        }
        return parser;
    };

    protected MultiValuesSourceFieldConfig(
        String fieldName,
        Object missing,
        Script script,
        ZoneId timeZone,
        QueryBuilder filter,
        ValueType userValueTypeHint,
        String format
    ) {
        this.fieldName = fieldName;
        this.missing = missing;
        this.script = script;
        this.timeZone = timeZone;
        this.filter = filter;
        this.userValueTypeHint = userValueTypeHint;
        this.format = format;
    }

    public MultiValuesSourceFieldConfig(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            this.fieldName = in.readOptionalString();
        } else {
            this.fieldName = in.readString();
        }
        this.missing = in.readGenericValue();
        this.script = in.readOptionalWriteable(Script::new);
        this.timeZone = in.readOptionalZoneId();
        if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
            this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        } else {
            this.filter = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_12_0)) {
            this.userValueTypeHint = in.readOptionalWriteable(ValueType::readFromStream);
            this.format = in.readOptionalString();
        } else {
            this.userValueTypeHint = null;
            this.format = null;
        }
    }

    public Object getMissing() {
        return missing;
    }

    public Script getScript() {
        return script;
    }

    public ZoneId getTimeZone() {
        return timeZone;
    }

    public String getFieldName() {
        return fieldName;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    public ValueType getUserValueTypeHint() {
        return userValueTypeHint;
    }

    public String getFormat() {
        return format;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            out.writeOptionalString(fieldName);
        } else {
            out.writeString(fieldName);
        }
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(script);
        out.writeOptionalZoneId(timeZone);
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            out.writeOptionalNamedWriteable(filter);
        }
        if (out.getVersion().onOrAfter(Version.V_7_12_0)) {
            out.writeOptionalWriteable(userValueTypeHint);
            out.writeOptionalString(format);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (missing != null) {
            builder.field(ParseField.CommonFields.MISSING.getPreferredName(), missing);
        }
        if (script != null) {
            builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        }
        if (fieldName != null) {
            builder.field(ParseField.CommonFields.FIELD.getPreferredName(), fieldName);
        }
        if (timeZone != null) {
            builder.field(ParseField.CommonFields.TIME_ZONE.getPreferredName(), timeZone.getId());
        }
        if (filter != null) {
            builder.field(FILTER.getPreferredName());
            filter.toXContent(builder, params);
        }
        if (userValueTypeHint != null) {
            builder.field(AggregationBuilder.CommonFields.VALUE_TYPE.getPreferredName(), userValueTypeHint.getPreferredName());
        }
        if (format != null) {
            builder.field(AggregationBuilder.CommonFields.FORMAT.getPreferredName(), format);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiValuesSourceFieldConfig that = (MultiValuesSourceFieldConfig) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(missing, that.missing)
            && Objects.equals(script, that.script)
            && Objects.equals(timeZone, that.timeZone)
            && Objects.equals(filter, that.filter)
            && Objects.equals(userValueTypeHint, that.userValueTypeHint)
            && Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, missing, script, timeZone, filter, userValueTypeHint, format);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {
        private String fieldName;
        private Object missing = null;
        private Script script = null;
        private ZoneId timeZone = null;
        private QueryBuilder filter = null;
        private ValueType userValueTypeHint = null;
        private String format = null;

        public String getFieldName() {
            return fieldName;
        }

        public Builder setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Object getMissing() {
            return missing;
        }

        public Builder setMissing(Object missing) {
            this.missing = missing;
            return this;
        }

        public Script getScript() {
            return script;
        }

        public Builder setScript(Script script) {
            this.script = script;
            return this;
        }

        public ZoneId getTimeZone() {
            return timeZone;
        }

        public Builder setTimeZone(ZoneId timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder setFilter(QueryBuilder filter) {
            this.filter = filter;
            return this;
        }

        public Builder setUserValueTypeHint(ValueType userValueTypeHint) {
            this.userValueTypeHint = userValueTypeHint;
            return this;
        }

        public ValueType getUserValueTypeHint() {
            return userValueTypeHint;
        }

        public Builder setFormat(String format) {
            this.format = format;
            return this;
        }

        public String getFormat() {
            return format;
        }

        public MultiValuesSourceFieldConfig build() {
            if (Strings.isNullOrEmpty(fieldName) && script == null) {
                throw new IllegalArgumentException(
                    "["
                        + ParseField.CommonFields.FIELD.getPreferredName()
                        + "] and ["
                        + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                        + "] cannot both be null.  "
                        + "Please specify one or the other."
                );
            }

            if (Strings.isNullOrEmpty(fieldName) == false && script != null) {
                throw new IllegalArgumentException(
                    "["
                        + ParseField.CommonFields.FIELD.getPreferredName()
                        + "] and ["
                        + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                        + "] cannot both be configured.  "
                        + "Please specify one or the other."
                );
            }

            return new MultiValuesSourceFieldConfig(fieldName, missing, script, timeZone, filter, userValueTypeHint, format);
        }
    }
}
