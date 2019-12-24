/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TimeSyncConfig  implements SyncConfig {

    public static final TimeValue DEFAULT_DELAY = TimeValue.timeValueSeconds(60);
    private static final String NAME = "data_frame_transform_pivot_sync_time";

    private final String field;
    private final TimeValue delay;

    private static final ConstructingObjectParser<TimeSyncConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TimeSyncConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<TimeSyncConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TimeSyncConfig, Void> parser = new ConstructingObjectParser<>(NAME, lenient,
            args -> {
                String field = (String) args[0];
                TimeValue delay = (TimeValue) args[1];
                return new TimeSyncConfig(field, delay);
            });
        parser.declareString(constructorArg(), TransformField.FIELD);
        parser.declareField(optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), DEFAULT_DELAY, TransformField.DELAY.getPreferredName()),
            TransformField.DELAY,
            ObjectParser.ValueType.STRING);
        return parser;
    }

    public TimeSyncConfig() {
        this(null, null);
    }

    public TimeSyncConfig(final String field, final TimeValue delay) {
        this.field = ExceptionsHelper.requireNonNull(field, TransformField.FIELD.getPreferredName());
        this.delay = delay == null ? DEFAULT_DELAY : delay;
    }

    public TimeSyncConfig(StreamInput in) throws IOException {
        this.field = in.readString();
        this.delay = in.readTimeValue();
    }

    public String getField() {
        return field;
    }

    public TimeValue getDelay() {
        return delay;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeTimeValue(delay);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.FIELD.getPreferredName(), field);
        builder.field(TransformField.DELAY.getPreferredName(), delay.getStringRep());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final TimeSyncConfig that = (TimeSyncConfig) other;

        return Objects.equals(this.field, that.field)
                && Objects.equals(this.delay, that.delay);
    }

    @Override
    public int hashCode(){
        return Objects.hash(field, delay);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static TimeSyncConfig parse(final XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    public static TimeSyncConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    @Override
    public String getWriteableName() {
        return TransformField.TIME_BASED_SYNC.getPreferredName();
    }

    @Override
    public QueryBuilder getRangeQuery(TransformCheckpoint newCheckpoint) {
        return new RangeQueryBuilder(field).lt(newCheckpoint.getTimeUpperBound()).format("epoch_millis");
    }

    @Override
    public QueryBuilder getRangeQuery(TransformCheckpoint oldCheckpoint, TransformCheckpoint newCheckpoint) {
        return new RangeQueryBuilder(field).gte(oldCheckpoint.getTimeUpperBound()).lt(newCheckpoint.getTimeUpperBound())
                .format("epoch_millis");
    }
}
