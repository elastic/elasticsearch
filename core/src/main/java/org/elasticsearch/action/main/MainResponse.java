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

package org.elasticsearch.action.main;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class MainResponse extends ActionResponse implements ToXContentObject {

    private String nodeName;
    private Version version;
    private ClusterName clusterName;
    private String clusterUuid;
    private Build build;
    boolean available;

    MainResponse() {
    }

    public MainResponse(String nodeName, Version version, ClusterName clusterName, String clusterUuid, Build build, boolean available) {
        this.nodeName = nodeName;
        this.version = version;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.build = build;
        this.available = available;
    }

    public String getNodeName() {
        return nodeName;
    }

    public Version getVersion() {
        return version;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public String getClusterUuid() {
        return clusterUuid;
    }

    public Build getBuild() {
        return build;
    }

    public boolean isAvailable() {
        return available;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeName);
        Version.writeVersion(version, out);
        clusterName.writeTo(out);
        out.writeString(clusterUuid);
        Build.writeBuild(build, out);
        out.writeBoolean(available);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodeName = in.readString();
        version = Version.readVersion(in);
        clusterName = new ClusterName(in);
        clusterUuid = in.readString();
        build = Build.readBuild(in);
        available = in.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", nodeName);
        builder.field("cluster_name", clusterName.value());
        builder.field("cluster_uuid", clusterUuid);
        builder.startObject("version")
            .field("number", version.toString())
            .field("build_hash", build.shortHash())
            .field("build_date", build.date())
            .field("build_snapshot", build.isSnapshot())
            .field("lucene_version", version.luceneVersion.toString())
            .field("minimum_wire_compatibility_version", version.minimumCompatibilityVersion().toString())
            .field("minimum_index_compatibility_version", version.minimumIndexCompatibilityVersion().toString())
            .endObject();
        builder.field("tagline", "You Know, for Search");
        builder.endObject();
        return builder;
    }

    private static final ObjectParser<MainResponse, Void> PARSER = new ObjectParser<>(MainResponse.class.getName(), true,
            MainResponse::new);

    static {
        PARSER.declareString((response, value) -> response.nodeName = value, new ParseField("name"));
        PARSER.declareString((response, value) -> response.clusterName = new ClusterName(value), new ParseField("cluster_name"));
        PARSER.declareString((response, value) -> response.clusterUuid = value, new ParseField("cluster_uuid"));
        PARSER.declareString((response, value) -> {}, new ParseField("tagline"));
        PARSER.declareObject((response, value) -> {
            response.build = new Build((String) value.get("build_hash"), (String) value.get("build_date"),
                    (boolean) value.get("build_snapshot"));
            response.version = Version.fromString((String) value.get("number"));
            response.available = true;
        }, (parser, context) -> parser.map(), new ParseField("version"));
    }

    public static MainResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MainResponse other = (MainResponse) o;
        return Objects.equals(nodeName, other.nodeName) &&
                Objects.equals(version, other.version) &&
                Objects.equals(clusterUuid, other.clusterUuid) &&
                Objects.equals(build, other.build) &&
                Objects.equals(available, other.available) &&
                Objects.equals(clusterName, other.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, version, clusterUuid, build, clusterName, available);
    }
}
