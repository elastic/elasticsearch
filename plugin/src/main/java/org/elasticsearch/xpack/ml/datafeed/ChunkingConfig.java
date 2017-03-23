/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * The description of how searches should be chunked.
 */
public class ChunkingConfig extends ToXContentToBytes implements Writeable {

    public static final ParseField MODE_FIELD = new ParseField("mode");
    public static final ParseField TIME_SPAN_FIELD = new ParseField("time_span");

    public static final ConstructingObjectParser<ChunkingConfig, Void> PARSER = new ConstructingObjectParser<>(
            "chunking_config", a -> new ChunkingConfig((Mode) a[0], (TimeValue) a[1]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Mode.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, MODE_FIELD, ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return TimeValue.parseTimeValue(p.text(), TIME_SPAN_FIELD.getPreferredName());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, TIME_SPAN_FIELD, ValueType.STRING);
    }

    private final Mode mode;
    private final TimeValue timeSpan;

    public ChunkingConfig(StreamInput in) throws IOException {
        mode = Mode.readFromStream(in);
        timeSpan = in.readOptionalWriteable(TimeValue::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        mode.writeTo(out);
        out.writeOptionalWriteable(timeSpan);
    }

    ChunkingConfig(Mode mode, @Nullable TimeValue timeSpan) {
        this.mode = ExceptionsHelper.requireNonNull(mode, MODE_FIELD.getPreferredName());
        this.timeSpan = timeSpan;
        if (mode == Mode.MANUAL) {
            if (timeSpan == null) {
                throw new IllegalArgumentException("when chunk mode is manual time_span is required");
            }
            if (timeSpan.getMillis() <= 0) {
                throw new IllegalArgumentException("chunk time_span has to be positive");
            }
        } else {
            if (timeSpan != null) {
                throw new IllegalArgumentException("chunk time_span may only be set when mode is manual");
            }
        }
    }

    @Nullable
    public TimeValue getTimeSpan() {
        return timeSpan;
    }

    public boolean isEnabled() {
        return mode != Mode.OFF;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODE_FIELD.getPreferredName(), mode);
        if (timeSpan != null) {
            builder.field(TIME_SPAN_FIELD.getPreferredName(), timeSpan.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, timeSpan);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        ChunkingConfig other = (ChunkingConfig) obj;
        return Objects.equals(this.mode, other.mode) &&
                Objects.equals(this.timeSpan, other.timeSpan);
    }

    public static ChunkingConfig newAuto() {
        return new ChunkingConfig(Mode.AUTO, null);
    }

    public static ChunkingConfig newOff() {
        return new ChunkingConfig(Mode.OFF, null);
    }

    public static ChunkingConfig newManual(TimeValue timeSpan) {
        return new ChunkingConfig(Mode.MANUAL, timeSpan);
    }

    public enum Mode implements Writeable  {
        AUTO, MANUAL, OFF;

        public static Mode fromString(String value) {
            return Mode.valueOf(value.toUpperCase(Locale.ROOT));
        }

        public static Mode readFromStream(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown Mode ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
