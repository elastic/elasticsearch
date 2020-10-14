/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

class SnapshotFeatureInfo implements Writeable, ToXContentObject {
    final String pluginName;
    final List<String> indices;

    static final ConstructingObjectParser<SnapshotFeatureInfo, Void> SNAPSHOT_PLUGIN_INFO_PARSER =
        new ConstructingObjectParser<>("plugin_info", true, (a, name) -> {
            String pluginName = (String) a[0];
            List<String> indices = (List<String>) a[1];
            return new SnapshotFeatureInfo(pluginName, indices);
        });

    static {
        SNAPSHOT_PLUGIN_INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("plugin_name"));
        SNAPSHOT_PLUGIN_INFO_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), new ParseField("indices"));
    }

    SnapshotFeatureInfo(String pluginName, List<String> indices) {
        this.pluginName = pluginName;
        this.indices = indices;
    }

    SnapshotFeatureInfo(final StreamInput in) throws IOException {
        this.pluginName = in.readString();
        this.indices = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pluginName);
        out.writeStringCollection(indices);
    }

    public static SnapshotFeatureInfo fromXContent(XContentParser parser) throws IOException {
        return SNAPSHOT_PLUGIN_INFO_PARSER.parse(parser, null);
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
            builder.field("plugin_name", pluginName);
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
        if (!(o instanceof SnapshotFeatureInfo)) return false;
        SnapshotFeatureInfo that = (SnapshotFeatureInfo) o;
        return getPluginName().equals(that.getPluginName()) &&
            getIndices().equals(that.getIndices());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPluginName(), getIndices());
    }
}
