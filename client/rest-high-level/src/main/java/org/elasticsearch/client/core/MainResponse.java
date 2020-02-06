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

package org.elasticsearch.client.core;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

public class MainResponse {

    private static final ConstructingObjectParser<MainResponse, Void> PARSER =
        new ConstructingObjectParser<>(MainResponse.class.getName(), true,
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

    private final String nodeName;
    private final Version version;
    private final String clusterName;
    private final String clusterUuid;
    private final String tagline;

    public MainResponse(String nodeName, Version version, String clusterName, String clusterUuid, String tagline) {
        this.nodeName = nodeName;
        this.version = version;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.tagline = tagline;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MainResponse that = (MainResponse) o;
        return nodeName.equals(that.nodeName) &&
            version.equals(that.version) &&
            clusterName.equals(that.clusterName) &&
            clusterUuid.equals(that.clusterUuid) &&
            tagline.equals(that.tagline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, version, clusterName, clusterUuid, tagline);
    }

    public static class Version {
        private static final ConstructingObjectParser<Version, Void> PARSER =
            new ConstructingObjectParser<>(Version.class.getName(), true,
                args -> {
                    return new Version((String) args[0], (String) args[1], (String) args[2], (String) args[3], (String) args[4],
                        (Boolean) args[5], (String) args[6], (String) args[7], (String) args[8]);
                }
            );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("number"));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("build_flavor"));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("build_type"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("build_hash"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("build_date"));
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField("build_snapshot"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("lucene_version"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("minimum_wire_compatibility_version"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("minimum_index_compatibility_version"));
        }
        private final String number;
        private final String buildFlavor;
        private final String buildType;
        private final String buildHash;
        private final String buildDate;
        private final boolean isSnapshot;
        private final String luceneVersion;
        private final String minimumWireCompatibilityVersion;
        private final String minimumIndexCompatibilityVersion;

        public Version(String number, String buildFlavor, String buildType, String buildHash, String buildDate, boolean isSnapshot,
            String luceneVersion, String minimumWireCompatibilityVersion, String minimumIndexCompatibilityVersion) {
            this.number = number;
            this.buildFlavor = buildFlavor;
            this.buildType = buildType;
            this.buildHash = buildHash;
            this.buildDate = buildDate;
            this.isSnapshot = isSnapshot;
            this.luceneVersion = luceneVersion;
            this.minimumWireCompatibilityVersion = minimumWireCompatibilityVersion;
            this.minimumIndexCompatibilityVersion = minimumIndexCompatibilityVersion;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Version version = (Version) o;
            return isSnapshot == version.isSnapshot &&
                number.equals(version.number) &&
                Objects.equals(buildFlavor, version.buildFlavor) &&
                Objects.equals(buildType, version.buildType) &&
                buildHash.equals(version.buildHash) &&
                buildDate.equals(version.buildDate) &&
                luceneVersion.equals(version.luceneVersion) &&
                minimumWireCompatibilityVersion.equals(version.minimumWireCompatibilityVersion) &&
                minimumIndexCompatibilityVersion.equals(version.minimumIndexCompatibilityVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(number, buildFlavor, buildType, buildHash, buildDate, isSnapshot, luceneVersion,
                minimumWireCompatibilityVersion, minimumIndexCompatibilityVersion);
        }
    }
}
