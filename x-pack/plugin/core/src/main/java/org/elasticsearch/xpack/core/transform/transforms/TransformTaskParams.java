/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

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
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;

public class TransformTaskParams extends AbstractDiffable<TransformTaskParams> implements PersistentTaskParams {

    public static final String NAME = TransformField.TASK_NAME;
    public static final ParseField FREQUENCY = TransformField.FREQUENCY;

    private final String transformId;
    private final Version version;
    private final TimeValue frequency;

    public static final ConstructingObjectParser<TransformTaskParams, Void> PARSER = new ConstructingObjectParser<>(NAME, true,
            a -> new TransformTaskParams((String) a[0], (String) a[1], (String) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TransformField.ID);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TransformField.VERSION);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FREQUENCY);
    }

    private TransformTaskParams(String transformId, String version, String frequency) {
        this(transformId, version == null ? null : Version.fromString(version),
            frequency == null ? null : TimeValue.parseTimeValue(frequency, FREQUENCY.getPreferredName()));
    }

    public TransformTaskParams(String transformId, Version version, TimeValue frequency) {
        this.transformId = transformId;
        this.version = version == null ? Version.V_7_2_0 : version;
        this.frequency = frequency;
    }

    public TransformTaskParams(StreamInput in) throws IOException {
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
        builder.field(TransformField.ID.getPreferredName(), transformId);
        builder.field(TransformField.VERSION.getPreferredName(), version);
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

    public static TransformTaskParams fromXContent(XContentParser parser) throws IOException {
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

        TransformTaskParams that = (TransformTaskParams) other;

        return Objects.equals(this.transformId, that.transformId)
            && Objects.equals(this.version, that.version)
            && Objects.equals(this.frequency, that.frequency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformId, version, frequency);
    }
}
