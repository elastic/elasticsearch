/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class MainResponse extends ActionResponse implements ToXContentObject {

    private String nodeName;
    private String luceneVersion;
    private ClusterName clusterName;
    private String clusterUuid;
    private Build build;

    MainResponse() {}

    MainResponse(StreamInput in) throws IOException {
        super(in);
        nodeName = in.readString();
        if (in.getTransportVersion().before(TransportVersions.V_8_500_041)) {
            Version.readVersion(in);
        }

        // Index version and transport version were briefly included in the main response, but
        // removed before the 8.9.0 release. Reading code remains here (throwing away the values)
        // for those versions until the new format has propagated through serverless. Additionally,
        // the lucene version was previously read by inferring from either Version or IndexVersion.
        // Now the lucene version is read explicitly.
        String wireLuceneVersion = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_500_037)) {
            wireLuceneVersion = in.readString();
        } else {
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_500_031)) {
                wireLuceneVersion = IndexVersion.readVersion(in).luceneVersion().toString();
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_500_020)) {
                TransportVersion.readVersion(in);
            }
        }
        clusterName = new ClusterName(in);
        clusterUuid = in.readString();
        build = Build.readBuild(in);

        if (wireLuceneVersion == null) {
            wireLuceneVersion = IndexVersion.fromId(Version.fromString(build.version()).id).luceneVersion().toString();
        }
        luceneVersion = wireLuceneVersion;
    }

    public MainResponse(String nodeName, String luceneVersion, ClusterName clusterName, String clusterUuid, Build build) {
        this.nodeName = nodeName;
        this.luceneVersion = luceneVersion;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.build = build;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getLuceneVersion() {
        return luceneVersion;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        if (out.getTransportVersion().before(TransportVersions.V_8_500_041)) {
            Version.writeVersion(Version.CURRENT, out);
        }

        // Index version and transport version were briefly included in the main response, but
        // removed before the 8.9.0 release. Writing code remains here (writing the latest versions)
        // for those versions until the new format has propagated through serverless. Additionally,
        // the lucene version was previously inferred from either Version or IndexVersion.
        // Now the lucene version is written explicitly.
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_500_037)) {
            out.writeString(luceneVersion);
        } else {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_500_031)) {
                IndexVersion.writeVersion(IndexVersion.current(), out);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_500_020)) {
                TransportVersion.writeVersion(TransportVersion.current(), out);
            }
        }
        clusterName.writeTo(out);
        out.writeString(clusterUuid);
        Build.writeBuild(build, out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", nodeName);
        builder.field("cluster_name", clusterName.value());
        builder.field("cluster_uuid", clusterUuid);
        builder.startObject("version")
            .field("number", build.qualifiedVersion())
            .field("build_flavor", build.flavor())
            .field("build_type", build.type().displayName())
            .field("build_hash", build.hash())
            .field("build_date", build.date())
            .field("build_snapshot", build.isSnapshot())
            .field("lucene_version", luceneVersion)
            .field("minimum_wire_compatibility_version", build.minWireCompatVersion())
            .field("minimum_index_compatibility_version", build.minIndexCompatVersion())
            .endObject();
        builder.field("tagline", "You Know, for Search");
        builder.endObject();
        return builder;
    }

    private static final ObjectParser<MainResponse, Void> PARSER = new ObjectParser<>(
        MainResponse.class.getName(),
        true,
        MainResponse::new
    );

    static {
        PARSER.declareString((response, value) -> response.nodeName = value, new ParseField("name"));
        PARSER.declareString((response, value) -> response.clusterName = new ClusterName(value), new ParseField("cluster_name"));
        PARSER.declareString((response, value) -> response.clusterUuid = value, new ParseField("cluster_uuid"));
        PARSER.declareString((response, value) -> {}, new ParseField("tagline"));
        PARSER.declareObject((response, value) -> {
            final String buildFlavor = (String) value.get("build_flavor");
            final String buildTypeStr = (String) value.get("build_type");
            // Be lenient when reading on the wire, the enumeration values from other versions might be different than what we know.
            final Build.Type buildType = buildTypeStr == null ? Build.Type.UNKNOWN : Build.Type.fromDisplayName(buildTypeStr, false);
            final String buildHash = (String) value.get("build_hash");
            final String buildDate = (String) value.get("build_date");
            final boolean isSnapshot = (boolean) value.get("build_snapshot");
            final String version = (String) value.get("number");
            final String minWireCompatVersion = (String) value.get("minimum_wire_compatibility_version");
            final String minIndexCompatVersion = (String) value.get("minimum_index_compatibility_version");
            final String displayString = Build.defaultDisplayString(buildType, buildHash, buildDate, version);

            response.build = new Build(
                buildFlavor,
                buildType,
                buildHash,
                buildDate,
                isSnapshot,
                version,
                minWireCompatVersion,
                minIndexCompatVersion,
                displayString
            );
            response.luceneVersion = ((String) value.get("lucene_version"));
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
        return Objects.equals(nodeName, other.nodeName)
            && Objects.equals(luceneVersion, other.luceneVersion)
            && Objects.equals(clusterUuid, other.clusterUuid)
            && Objects.equals(build, other.build)
            && Objects.equals(clusterName, other.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, luceneVersion, clusterUuid, build, clusterName);
    }

    @Override
    public String toString() {
        return "MainResponse{"
            + "nodeName='"
            + nodeName
            + '\''
            + ", luceneVersion="
            + luceneVersion
            + ", clusterName="
            + clusterName
            + ", clusterUuid='"
            + clusterUuid
            + '\''
            + ", build="
            + build
            + '}';
    }
}
