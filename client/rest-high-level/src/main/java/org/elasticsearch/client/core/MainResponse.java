/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

public record MainResponse(String nodeName, Version version, String clusterName, String clusterUuid, String tagline) {

    private static final ConstructingObjectParser<MainResponse, Void> PARSER = new ConstructingObjectParser<>(
        MainResponse.class.getName(),
        true,
        args -> {
            return new MainResponse((String) args[0], (Version) args[1], (String) args[2], (String) args[3], (String) args[4]);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("name"));
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), Version.PARSER, new ParseField("version"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("cluster_name"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("cluster_uuid"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("tagline"));
    }

    public String getNodeName() {
        return nodeName;
    }

    public Version getVersion() {
        return version;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterUuid() {
        return clusterUuid;
    }

    public String getTagline() {
        return tagline;
    }

    public static MainResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public record Version(
        String number,
        String buildFlavor,
        String buildType,
        String buildHash,
        String buildDate,
        boolean isSnapshot,
        String luceneVersion,
        String indexVersion,
        String minimumWireCompatibilityVersion,
        String minimumIndexCompatibilityVersion,
        String transportVersion
    ) {

        private static final ConstructingObjectParser<Version, Void> PARSER = new ConstructingObjectParser<>(
            Version.class.getName(),
            true,
            args -> new Version(
                (String) args[0],
                (String) args[1],
                (String) args[2],
                (String) args[3],
                (String) args[4],
                (Boolean) args[5],
                (String) args[6],
                (String) args[7],
                (String) args[8],
                (String) args[9],
                (String) args[10]
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("number"));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("build_flavor"));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("build_type"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("build_hash"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("build_date"));
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField("build_snapshot"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("lucene_version"));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("index_version"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("minimum_wire_compatibility_version"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("minimum_index_compatibility_version"));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("transport_version"));
        }

        public String getNumber() {
            return number;
        }

        public String getBuildFlavor() {
            return buildFlavor;
        }

        public String getBuildType() {
            return buildType;
        }

        public String getBuildHash() {
            return buildHash;
        }

        public String getBuildDate() {
            return buildDate;
        }

        public boolean isSnapshot() {
            return isSnapshot;
        }

        public String getLuceneVersion() {
            return luceneVersion;
        }

        public String getMinimumWireCompatibilityVersion() {
            return minimumWireCompatibilityVersion;
        }

        public String getMinimumIndexCompatibilityVersion() {
            return minimumIndexCompatibilityVersion;
        }
    }
}
