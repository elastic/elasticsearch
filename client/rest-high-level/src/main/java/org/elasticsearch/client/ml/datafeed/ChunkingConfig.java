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
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

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
