/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * The description of how searches should be chunked.
 */
public class ChunkingConfig implements ToXContentObject {

    public static final ParseField MODE_FIELD = new ParseField("mode");
    public static final ParseField TIME_SPAN_FIELD = new ParseField("time_span");

    public static final ConstructingObjectParser<ChunkingConfig, Void> PARSER = new ConstructingObjectParser<>(
        "chunking_config", true, a -> new ChunkingConfig((Mode) a[0], (TimeValue) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Mode::fromString, MODE_FIELD);
        PARSER.declareString(
            ConstructingObjectParser.optionalConstructorArg(),
            text -> TimeValue.parseTimeValue(text, TIME_SPAN_FIELD.getPreferredName()),
            TIME_SPAN_FIELD);

    }

    private final Mode mode;
    private final TimeValue timeSpan;


    ChunkingConfig(Mode mode, @Nullable TimeValue timeSpan) {
        this.mode = Objects.requireNonNull(mode, MODE_FIELD.getPreferredName());
        this.timeSpan = timeSpan;
    }

    @Nullable
    public TimeValue getTimeSpan() {
        return timeSpan;
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

        if (obj == null || getClass() != obj.getClass()) {
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

    public enum Mode {
        AUTO, MANUAL, OFF;

        public static Mode fromString(String value) {
            return Mode.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
