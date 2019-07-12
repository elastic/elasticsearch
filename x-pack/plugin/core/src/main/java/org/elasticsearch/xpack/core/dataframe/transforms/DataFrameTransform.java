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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Objects;

public class DataFrameTransform extends AbstractDiffable<DataFrameTransform> implements XPackPlugin.XPackPersistentTaskParams {

    public static final String NAME = DataFrameField.TASK_NAME;
    public static final ParseField VERSION = new ParseField(DataFrameField.VERSION);

    private final String transformId;
    private final Version version;

    public static final ConstructingObjectParser<DataFrameTransform, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new DataFrameTransform((String) a[0], (String) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameField.ID);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), VERSION);
    }

    private DataFrameTransform(String transformId, String version) {
        this(transformId, version == null ? null : Version.fromString(version));
    }

    public DataFrameTransform(String transformId, Version version) {
        this.transformId = transformId;
        this.version = version == null ? Version.V_7_2_0 : version;
    }

    public DataFrameTransform(StreamInput in) throws IOException {
        this.transformId  = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            this.version = Version.readVersion(in);
        } else {
            this.version = Version.V_7_2_0;
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), transformId);
        builder.field(VERSION.getPreferredName(), version);
        builder.endObject();
        return builder;
    }

    public String getId() {
        return transformId;
    }

    public Version getVersion() {
        return version;
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

        return Objects.equals(this.transformId, that.transformId) && Objects.equals(this.version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformId, version);
    }
}
