/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Objects;

public class DataFrameTransform extends AbstractDiffable<DataFrameTransform> implements PersistentTaskParams {

    public static final String NAME = DataFrameField.TASK_NAME;
    public static final ParseField FREQUENCY = DataFrameField.FREQUENCY;

    private final String transformId;
    private final Version version;
    private final TimeValue frequency;

    public static final ConstructingObjectParser<DataFrameTransform, Void> PARSER = new ConstructingObjectParser<>(NAME, true,
            a -> new DataFrameTransform((String) a[0], (String) a[1], (String) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameField.ID);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.VERSION);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FREQUENCY);
    }

    private DataFrameTransform(String transformId, String version, String frequency) {
        this(transformId, version == null ? null : Version.fromString(version),
            frequency == null ? null : TimeValue.parseTimeValue(frequency, FREQUENCY.getPreferredName()));
    }

    public DataFrameTransform(String transformId, Version version, TimeValue frequency) {
        this.transformId = transformId;
        this.version = version == null ? Version.V_7_2_0 : version;
        this.frequency = frequency;
    }

    public DataFrameTransform(StreamInput in) throws IOException {
        this.transformId  = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            this.version = Version.readVersion(in);
        } else {
            this.version = Version.V_7_2_0;
        }
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            this.frequency = in.readOptionalTimeValue();
        } else {
            this.frequency = null;
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_2_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(transformId);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            Version.writeVersion(version, out);
        }
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeOptionalTimeValue(frequency);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), transformId);
        builder.field(DataFrameField.VERSION.getPreferredName(), version);
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    public String getId() {
        return transformId;
    }

    public Version getVersion() {
        return version;
    }

    public TimeValue getFrequency() {
        return frequency;
    }

    public static DataFrameTransform fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransform that = (DataFrameTransform) other;

        return Objects.equals(this.transformId, that.transformId)
            && Objects.equals(this.version, that.version)
            && Objects.equals(this.frequency, that.frequency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformId, version, frequency);
    }
}
