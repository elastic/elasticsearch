/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TimeSyncConfig implements SyncConfig {

    public static final TimeValue DEFAULT_DELAY = TimeValue.timeValueSeconds(60);
    private static final String NAME = "data_frame_transform_pivot_sync_time";

    static final TransportVersion TRANSFORM_SYNC_INITIAL_DELAY = TransportVersion.fromName("transform_sync_initial_delay");

    private final String field;
    private final TimeValue delay;
    private final TimeValue initialDelay;

    private static final ConstructingObjectParser<TimeSyncConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TimeSyncConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<TimeSyncConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TimeSyncConfig, Void> parser = new ConstructingObjectParser<>(NAME, lenient, args -> {
            String field = (String) args[0];
            TimeValue delay = (TimeValue) args[1];
            TimeValue initialDelay = (TimeValue) args[2];
            return new TimeSyncConfig(field, delay, initialDelay);
        });
        parser.declareString(constructorArg(), TransformField.FIELD);
        parser.declareField(
            optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), DEFAULT_DELAY, TransformField.DELAY.getPreferredName()),
            TransformField.DELAY,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
            optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), TransformField.INITIAL_DELAY.getPreferredName()),
            TransformField.INITIAL_DELAY,
            ObjectParser.ValueType.STRING
        );
        return parser;
    }

    public TimeSyncConfig() {
        this(null, null, null);
    }

    public TimeSyncConfig(final String field, final TimeValue delay) {
        this(field, delay, null);
    }

    public TimeSyncConfig(final String field, final TimeValue delay, final TimeValue initialDelay) {
        this.field = ExceptionsHelper.requireNonNull(field, TransformField.FIELD.getPreferredName());
        this.delay = delay == null ? DEFAULT_DELAY : delay;
        this.initialDelay = initialDelay == null ? this.delay : initialDelay;
        if (this.initialDelay.compareTo(this.delay) > 0) {
            throw new IllegalArgumentException(
                "["
                    + TransformField.INITIAL_DELAY.getPreferredName()
                    + "] ["
                    + this.initialDelay.getStringRep()
                    + "] must not be greater than ["
                    + TransformField.DELAY.getPreferredName()
                    + "] ["
                    + this.delay.getStringRep()
                    + "]"
            );
        }
    }

    public TimeSyncConfig(StreamInput in) throws IOException {
        this.field = in.readString();
        this.delay = in.readTimeValue();
        if (in.getTransportVersion().supports(TRANSFORM_SYNC_INITIAL_DELAY)) {
            this.initialDelay = in.readTimeValue();
        } else {
            this.initialDelay = this.delay;
        }
    }

    @Override
    public String getField() {
        return field;
    }

    public TimeValue getDelay() {
        return delay;
    }

    /**
     * @return the reduced delay applied while the transform is catching up; equal to {@link #getDelay()} unless an explicit
     *         {@code initial_delay} was configured.
     */
    public TimeValue getInitialDelay() {
        return initialDelay;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeTimeValue(delay);
        if (out.getTransportVersion().supports(TRANSFORM_SYNC_INITIAL_DELAY)) {
            out.writeTimeValue(initialDelay);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.FIELD.getPreferredName(), field);
        builder.field(TransformField.DELAY.getPreferredName(), delay.getStringRep());
        // Only emit initial_delay when it actually differs from delay so that the output of existing transforms is unchanged.
        if (initialDelay.equals(delay) == false) {
            builder.field(TransformField.INITIAL_DELAY.getPreferredName(), initialDelay.getStringRep());
        }
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
            && Objects.equals(this.delay, that.delay)
            && Objects.equals(this.initialDelay, that.initialDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, delay, initialDelay);
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
        return TransformField.TIME.getPreferredName();
    }

    @Override
    public QueryBuilder getRangeQuery(TransformCheckpoint newCheckpoint) {
        return new RangeQueryBuilder(field).lt(newCheckpoint.getTimeUpperBound()).format("epoch_millis");
    }

    @Override
    public QueryBuilder getRangeQuery(TransformCheckpoint oldCheckpoint, TransformCheckpoint newCheckpoint) {
        return new RangeQueryBuilder(field).gte(oldCheckpoint.getTimeUpperBound())
            .lt(newCheckpoint.getTimeUpperBound())
            .format("epoch_millis");
    }
}
