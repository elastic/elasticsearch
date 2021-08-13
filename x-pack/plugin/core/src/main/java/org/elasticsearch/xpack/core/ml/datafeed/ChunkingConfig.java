/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * The description of how searches should be chunked.
 */
public class ChunkingConfig implements ToXContentObject, Writeable {

    public static final ParseField MODE_FIELD = new ParseField("mode");
    public static final ParseField TIME_SPAN_FIELD = new ParseField("time_span");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<ChunkingConfig, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<ChunkingConfig, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<ChunkingConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<ChunkingConfig, Void> parser = new ConstructingObjectParser<>(
            "chunking_config", ignoreUnknownFields, a -> new ChunkingConfig((Mode) a[0], (TimeValue) a[1]));

        parser.declareString(ConstructingObjectParser.constructorArg(), Mode::fromString, MODE_FIELD);
        parser.declareString(
            ConstructingObjectParser.optionalConstructorArg(),
            text -> TimeValue.parseTimeValue(text, TIME_SPAN_FIELD.getPreferredName()),
            TIME_SPAN_FIELD);

        return parser;
    }

    private final Mode mode;
    private final TimeValue timeSpan;

    public ChunkingConfig(StreamInput in) throws IOException {
        mode = Mode.readFromStream(in);
        timeSpan = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        mode.writeTo(out);
        out.writeOptionalTimeValue(timeSpan);
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

    public boolean isManual() {
        return mode == Mode.MANUAL;
    }

    Mode getMode() {
        return mode;
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
            return in.readEnum(Mode.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
