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
import org.elasticsearch.common.Strings;
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
    private String clusterDynamicName;
    private Build build;

    MainResponse() {
    }

    public MainResponse(String nodeName, Version version, ClusterName clusterName, String clusterUuid,
                        String clusterDynamicName, Build build) {
        this.nodeName = nodeName;
        this.version = version;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.clusterDynamicName = clusterDynamicName;
        this.build = build;
    }

    /**
     * Retrieves the `node.name` parameter configured on the node answering this request
     * @return the value of `node.name`
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Retrieves the version of Elasticsearch running on the node answering this request
     * @return the installed Elasticsearch version
     */
    public Version getVersion() {
        return version;
    }

    /**
     * Retrieves the `cluster.name` parameter configured on the node answering this request.
     * Note that `cluster.name` will be identical across all nodes in the cluster.
     * @return the value of `cluster.name`
     */
    public ClusterName getClusterName() {
        return clusterName;
    }

    /**
     * Retrieves the generated universally-unique identifier for the Elasticsearch cluster
     * the queried node is a part of
     * @return the UUID
     */
    public String getClusterUuid() {
        return clusterUuid;
    }

    /**
     * Retrieves the `cluster.dynamic_name` parameter configured on the cluster.  This is
     * a dynamic setting that may be updated by the user on a running cluster.  This field
     * exists to provide users a way to dynamically assign a friendly identifier to a cluster.
     * @return
     */
    public String getClusterDynamicName() { return clusterDynamicName; }

    /**
     * Retrieves information about the build of Elasticsearch running on the node answering
     * this request.
     * @return the installed Elasticsearch build information
     */
    public Build getBuild() {
        return build;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeName);
        Version.writeVersion(version, out);
        clusterName.writeTo(out);
        out.writeString(clusterUuid);
        Build.writeBuild(build, out);
        if (out.getVersion().before(Version.V_7_0_0_alpha1)) {
            out.writeBoolean(true);
        }
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            out.writeOptionalString(clusterDynamicName);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodeName = in.readString();
        version = Version.readVersion(in);
        clusterName = new ClusterName(in);
        clusterUuid = in.readString();
        build = Build.readBuild(in);
        if (in.getVersion().before(Version.V_7_0_0_alpha1)) {
            in.readBoolean();
        }
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            clusterDynamicName = in.readOptionalString();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", nodeName);
        builder.field("cluster_name", clusterName.value());
        builder.field("cluster_uuid", clusterUuid);
        if (false == Strings.isEmpty(clusterDynamicName)) {
            builder.field("cluster_dynamic_name", clusterDynamicName);
        }
        builder.startObject("version")
            .field("number", version.toString())
            .field("build_flavor", build.flavor().displayName())
            .field("build_type", build.type().displayName())
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
        PARSER.declareString((response, value) -> response.clusterDynamicName = value, new ParseField("cluster_dynamic_name"));
        PARSER.declareString((response, value) -> {}, new ParseField("tagline"));
        PARSER.declareObject((response, value) -> {
            final String buildFlavor = (String) value.get("build_flavor");
            final String buildType = (String) value.get("build_type");
            response.build =
                    new Build(
                            buildFlavor == null ? Build.Flavor.UNKNOWN : Build.Flavor.fromDisplayName(buildFlavor),
                            buildType == null ? Build.Type.UNKNOWN : Build.Type.fromDisplayName(buildType),
                            (String) value.get("build_hash"),
                            (String) value.get("build_date"),
                            (boolean) value.get("build_snapshot"));
            response.version = Version.fromString((String) value.get("number"));
        }, (parser, context) -> parser.map(), new ParseField("version"));
    }

    public static MainResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MainResponse that = (MainResponse) o;
        return Objects.equals(nodeName, that.nodeName) &&
            Objects.equals(version, that.version) &&
            Objects.equals(clusterName, that.clusterName) &&
            Objects.equals(clusterUuid, that.clusterUuid) &&
            Objects.equals(clusterDynamicName, that.clusterDynamicName) &&
            Objects.equals(build, that.build);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, version, clusterName, clusterUuid, clusterDynamicName, build);
    }
    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
