/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

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
            switch (id) {
                case 0:
                    return TERMS;
                case 1:
                    return HISTOGRAM;
                case 2:
                    return DATE_HISTOGRAM;
                case 3:
                    return GEOTILE_GRID;
                default:
                    throw new IllegalArgumentException("unknown type");
            }
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
    }

    public SingleGroupSource(final String field, final ScriptConfig scriptConfig, final boolean missingBucket) {
        this.field = field;
        this.scriptConfig = scriptConfig;
        this.missingBucket = missingBucket;
    }

    public SingleGroupSource(StreamInput in) throws IOException {
        field = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            scriptConfig = in.readOptionalWriteable(ScriptConfig::new);
        } else {
            scriptConfig = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            missingBucket = in.readBoolean();
        } else {
            missingBucket = false;
        }
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
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeOptionalWriteable(scriptConfig);
        }
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeBoolean(missingBucket);
        }
    }

    public abstract Type getType();

    public abstract boolean supportsIncrementalBucketUpdate();

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

    /**
     * This will transform a composite aggregation bucket key into the desired format for indexing.
     *
     * @param key The bucket key for this group source
     * @return the transformed bucket key for indexing
     */
    public Object transformBucketKey(Object key) {
        return key;
    }
}
