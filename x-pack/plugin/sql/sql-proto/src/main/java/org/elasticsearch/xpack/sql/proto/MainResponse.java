/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.Build;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

/**
 * Main (/) response for JDBC/CLI client
 */
public class MainResponse {
    private String nodeName;
    private String version;
    private String clusterName;
    private String clusterUuid;
    // TODO: Add parser for Build
    private Build build;

    private MainResponse() {
    }

    public MainResponse(String nodeName, String version, String clusterName, String clusterUuid, Build build) {
        this.nodeName = nodeName;
        this.version = version;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.build = build;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getVersion() {
        return version;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterUuid() {
        return clusterUuid;
    }

    public Build getBuild() {
        return build;
    }

    private static final ObjectParser<MainResponse, Void> PARSER = new ObjectParser<>(MainResponse.class.getName(), true,
        MainResponse::new);

    static {
        PARSER.declareString((response, value) -> response.nodeName = value, new ParseField("name"));
        PARSER.declareString((response, value) -> response.clusterName = value, new ParseField("cluster_name"));
        PARSER.declareString((response, value) -> response.clusterUuid = value, new ParseField("cluster_uuid"));
        PARSER.declareString((response, value) -> {
        }, new ParseField("tagline"));
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
            response.version = (String) value.get("number");
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
            Objects.equals(clusterName, other.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, version, clusterUuid, build, clusterName);
    }
}
