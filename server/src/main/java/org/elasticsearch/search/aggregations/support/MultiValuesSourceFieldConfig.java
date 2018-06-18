package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.MultiValueMode;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.function.BiFunction;

public class MultiValuesSourceFieldConfig implements Writeable, ToXContentFragment {
    private String fieldName;
    private Object missing = null;
    private Script script = null;
    private DateTimeZone timeZone = null;
    private MultiValueMode multi = MultiValueMode.AVG;

    private static final String NAME = "field_config";
    private static final ParseField MULTI = new ParseField("multi");

    public static final BiFunction<Boolean, Boolean, ConstructingObjectParser<MultiValuesSourceFieldConfig, Void>> PARSER
        = (scriptable, timezoneAware) -> {

        ConstructingObjectParser<MultiValuesSourceFieldConfig, Void> parser
            = new ConstructingObjectParser<>(MultiValuesSourceFieldConfig.NAME, false, o -> new MultiValuesSourceFieldConfig((String)o[0]));

        parser.declareString(ConstructingObjectParser.constructorArg(), ParseField.CommonFields.FIELD);
        parser.declareField(MultiValuesSourceFieldConfig::setMissing, XContentParser::objectText,
            ParseField.CommonFields.MISSING, ObjectParser.ValueType.VALUE);
        parser.declareField(MultiValuesSourceFieldConfig::setMulti, p -> MultiValueMode.fromString(p.text()), MULTI,
            ObjectParser.ValueType.STRING);

        if (scriptable) {
            parser.declareField(MultiValuesSourceFieldConfig::setScript,
                (p, context) -> Script.parse(p),
                Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);
        }

        if (timezoneAware) {
            parser.declareField(MultiValuesSourceFieldConfig::setTimeZone, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return DateTimeZone.forID(p.text());
                } else {
                    return DateTimeZone.forOffsetHours(p.intValue());
                }
            }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
        }
        return parser;
    };


    public MultiValuesSourceFieldConfig(String fieldName) {
        this.fieldName = fieldName;
    }

    public MultiValuesSourceFieldConfig(StreamInput in) throws IOException {
        this.fieldName = in.readString();
        this.missing = in.readGenericValue();
        this.script = in.readOptionalWriteable(Script::new);
        this.timeZone = in.readOptionalTimeZone();
        this.multi = MultiValueMode.readMultiValueModeFrom(in);
    }

    public Object getMissing() {
        return missing;
    }

    public MultiValuesSourceFieldConfig setMissing(Object missing) {
        this.missing = missing;
        return this;
    }

    public Script getScript() {
        return script;
    }

    public MultiValuesSourceFieldConfig setScript(Script script) {
        this.script = script;
        return this;
    }

    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    public MultiValuesSourceFieldConfig setTimeZone(DateTimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public String getFieldName() {
        return fieldName;
    }

    public MultiValuesSourceFieldConfig setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public MultiValueMode getMulti() {
        return multi;
    }

    public MultiValuesSourceFieldConfig setMulti(MultiValueMode multi) {
        this.multi = multi;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(script);
        out.writeOptionalTimeZone(timeZone);
        multi.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
            builder.field(ParseField.CommonFields.TIME_ZONE.getPreferredName(), timeZone);
        }
        if (multi != null) {
            builder.field(MULTI.getPreferredName(), multi);
        }
        return builder;
    }
}
