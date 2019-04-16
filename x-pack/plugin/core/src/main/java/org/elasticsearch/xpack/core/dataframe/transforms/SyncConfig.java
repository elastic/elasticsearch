/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SyncConfig implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_transform_pivot_sync";

    private final TimeSyncConfig timeSyncConfig;

    private static final ConstructingObjectParser<SyncConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<SyncConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<SyncConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<SyncConfig, Void> parser = new ConstructingObjectParser<>(NAME, lenient, args -> {
            TimeSyncConfig timeSync = (TimeSyncConfig) args[0];

            return new SyncConfig(timeSync);
        });

        parser.declareObject(optionalConstructorArg(), (p, c) -> TimeSyncConfig.fromXContent(p, lenient), DataFrameField.TIME_BASED_SYNC);

        return parser;
    }

    public SyncConfig(TimeSyncConfig timeSyncConfig) {
        this.timeSyncConfig = timeSyncConfig;
    }

    public SyncConfig(StreamInput in) throws IOException {
        this.timeSyncConfig = in.readOptionalWriteable(TimeSyncConfig::new);
    }

    public TimeSyncConfig getTimeSyncConfig() {
        return timeSyncConfig;
    }

    public boolean isValid() {
        if (timeSyncConfig != null && timeSyncConfig.isValid() == false) {
            return false;
        }

        return true;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalWriteable(timeSyncConfig);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (timeSyncConfig != null) {
            builder.field(DataFrameField.TIME_BASED_SYNC.getPreferredName(), timeSyncConfig);
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

        final SyncConfig that = (SyncConfig) other;

        return Objects.equals(this.timeSyncConfig, that.timeSyncConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeSyncConfig);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static SyncConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

}
