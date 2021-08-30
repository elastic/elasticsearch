/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SnapshotFeatureInfo implements Writeable, ToXContentObject {
    final String pluginName;
    final List<String> indices;

    static final ConstructingObjectParser<SnapshotFeatureInfo, Void> SNAPSHOT_FEATURE_INFO_PARSER = new ConstructingObjectParser<>(
        "feature_info",
        true,
        (a, name) -> {
            String pluginName = (String) a[0];
            @SuppressWarnings("unchecked")
            List<String> indices = (List<String>) a[1];
            return new SnapshotFeatureInfo(pluginName, indices);
        }
    );

    static {
        SNAPSHOT_FEATURE_INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("feature_name"));
        SNAPSHOT_FEATURE_INFO_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), new ParseField("indices"));
    }

    public SnapshotFeatureInfo(String pluginName, List<String> indices) {
        this.pluginName = pluginName;
        this.indices = org.elasticsearch.core.List.copyOf(indices);
    }

    public SnapshotFeatureInfo(final StreamInput in) throws IOException {
        this(in.readString(), in.readStringList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pluginName);
        out.writeStringCollection(indices);
    }

    public static SnapshotFeatureInfo fromXContent(XContentParser parser) throws IOException {
        return SNAPSHOT_FEATURE_INFO_PARSER.parse(parser, null);
    }

    public String getPluginName() {
        return pluginName;
    }

    public List<String> getIndices() {
        return indices;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("feature_name", pluginName);
            builder.startArray("indices");
            for (String index : indices) {
                builder.value(index);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof SnapshotFeatureInfo) == false) return false;
        SnapshotFeatureInfo that = (SnapshotFeatureInfo) o;
        return getPluginName().equals(that.getPluginName()) && getIndices().equals(that.getIndices());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPluginName(), getIndices());
    }
}
