/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/*
 * Base class for a single source for group_by
 */
public abstract class SingleGroupSource implements Writeable, ToXContentObject {

    public enum Type {
        TERMS(0),
        HISTOGRAM(1),
        DATE_HISTOGRAM(2),
        GEOTILE_GRID(3);

        private final byte id;

        Type(int id) {
            this.id = (byte) id;
        }

        public byte getId() {
            return id;
        }

        public static Type fromId(byte id) {
            return switch (id) {
                case 0 -> TERMS;
                case 1 -> HISTOGRAM;
                case 2 -> DATE_HISTOGRAM;
                case 3 -> GEOTILE_GRID;
                default -> throw new IllegalArgumentException("unknown type");
            };
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    protected static final ParseField FIELD = new ParseField("field");
    protected static final ParseField SCRIPT = new ParseField("script");
    protected static final ParseField MISSING_BUCKET = new ParseField("missing_bucket");

    protected final String field;
    protected final ScriptConfig scriptConfig;
    protected final boolean missingBucket;

    static <T> void declareValuesSourceFields(AbstractObjectParser<? extends SingleGroupSource, T> parser, boolean lenient) {
        parser.declareString(optionalConstructorArg(), FIELD);
        parser.declareObject(optionalConstructorArg(), (p, c) -> ScriptConfig.fromXContent(p, lenient), SCRIPT);
        parser.declareBoolean(optionalConstructorArg(), MISSING_BUCKET);
        if (lenient == false) {
            // either a script or a field must be declared, or both
            parser.declareRequiredFieldSet(FIELD.getPreferredName(), SCRIPT.getPreferredName());
        }
    }

    public SingleGroupSource(final String field, final ScriptConfig scriptConfig, final boolean missingBucket) {
        this.field = field;
        this.scriptConfig = scriptConfig;
        this.missingBucket = missingBucket;
    }

    public SingleGroupSource(StreamInput in) throws IOException {
        field = in.readOptionalString();
        scriptConfig = in.readOptionalWriteable(ScriptConfig::new);
        missingBucket = in.readBoolean();
    }

    ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        // either a script or a field must be declared
        if (field == null && scriptConfig == null) {
            validationException = addValidationError(
                "Required one of fields [field, script], but none were specified.",
                validationException
            );
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        if (scriptConfig != null) {
            builder.field(SCRIPT.getPreferredName(), scriptConfig);
        }
        if (missingBucket) {
            builder.field(MISSING_BUCKET.getPreferredName(), missingBucket);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(field);
        out.writeOptionalWriteable(scriptConfig);
        out.writeBoolean(missingBucket);
    }

    public abstract Type getType();

    public String getField() {
        return field;
    }

    public ScriptConfig getScriptConfig() {
        return scriptConfig;
    }

    public boolean getMissingBucket() {
        return missingBucket;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SingleGroupSource that = (SingleGroupSource) other;

        return this.missingBucket == that.missingBucket
            && Objects.equals(this.field, that.field)
            && Objects.equals(this.scriptConfig, that.scriptConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, scriptConfig, missingBucket);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    /**
     * @return The preferred mapping type if it exists. Is nullable.
     */
    @Nullable
    public String getMappingType() {
        return null;
    }

}
