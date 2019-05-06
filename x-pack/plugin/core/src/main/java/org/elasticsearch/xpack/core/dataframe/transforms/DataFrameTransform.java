/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
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

    private final String transformId;

    public static final ConstructingObjectParser<DataFrameTransform, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new DataFrameTransform((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameField.ID);
    }

    public DataFrameTransform(String transformId) {
        this.transformId = transformId;
    }

    public DataFrameTransform(StreamInput in) throws IOException {
        this.transformId  = in.readString();
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), transformId);
        builder.endObject();
        return builder;
    }

    public String getId() {
        return transformId;
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

        return Objects.equals(this.transformId, that.transformId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformId);
    }
}
