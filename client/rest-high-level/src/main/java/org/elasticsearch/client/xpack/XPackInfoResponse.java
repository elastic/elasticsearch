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
package org.elasticsearch.client.xpack;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.license.LicenseStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class XPackInfoResponse {
    /**
     * Value of the license's expiration time if it should never expire.
     */
    public static final long BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS = Long.MAX_VALUE - TimeUnit.HOURS.toMillis(24 * 365);
    // TODO move this constant to License.java once we move License.java to the protocol jar

    @Nullable private BuildInfo buildInfo;
    @Nullable private LicenseInfo licenseInfo;
    @Nullable private FeatureSetsInfo featureSetsInfo;

    public XPackInfoResponse() {}

    public XPackInfoResponse(@Nullable BuildInfo buildInfo, @Nullable LicenseInfo licenseInfo, @Nullable FeatureSetsInfo featureSetsInfo) {
        this.buildInfo = buildInfo;
        this.licenseInfo = licenseInfo;
        this.featureSetsInfo = featureSetsInfo;
    }

    /**
     * @return  The build info (incl. build hash and timestamp)
     */
    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    /**
     * @return  The current license info (incl. UID, type/mode. status and expiry date). May return {@code null} when no
     *          license is currently installed.
     */
    public LicenseInfo getLicenseInfo() {
        return licenseInfo;
    }

    /**
     * @return  The current status of the feature sets in X-Pack. Feature sets describe the features available/enabled in X-Pack.
     */
    public FeatureSetsInfo getFeatureSetsInfo() {
        return featureSetsInfo;
    }

    private static final ConstructingObjectParser<XPackInfoResponse, Void> PARSER = new ConstructingObjectParser<>(
            "xpack_info_response", true, (a, v) -> {
                BuildInfo buildInfo = (BuildInfo) a[0];
                LicenseInfo licenseInfo = (LicenseInfo) a[1];
                @SuppressWarnings("unchecked") // This is how constructing object parser works
                List<FeatureSetsInfo.FeatureSet> featureSets = (List<FeatureSetsInfo.FeatureSet>) a[2];
                FeatureSetsInfo featureSetsInfo = featureSets == null ? null : new FeatureSetsInfo(new HashSet<>(featureSets));
                return new XPackInfoResponse(buildInfo, licenseInfo, featureSetsInfo);
            });
    static {
        PARSER.declareObject(optionalConstructorArg(), BuildInfo.PARSER, new ParseField("build"));
        /*
         * licenseInfo is sort of "double optional" because it is
         * optional but it can also be send as `null`.
         */
        PARSER.declareField(optionalConstructorArg(), (p, v) -> {
                    if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                        return null;
                    }
                    return LicenseInfo.PARSER.parse(p, v);
                },
                new ParseField("license"), ValueType.OBJECT_OR_NULL);
        PARSER.declareNamedObjects(optionalConstructorArg(),
                (p, c, name) -> FeatureSetsInfo.FeatureSet.PARSER.parse(p, name),
                new ParseField("features"));
    }
    public static XPackInfoResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static class LicenseInfo {
        private final String uid;
        private final String type;
        private final String mode;
        private final LicenseStatus status;
        private final long expiryDate;

        public LicenseInfo(String uid, String type, String mode, LicenseStatus status, long expiryDate) {
            this.uid = uid;
            this.type = type;
            this.mode = mode;
            this.status = status;
            this.expiryDate = expiryDate;
        }

        public String getUid() {
            return uid;
        }

        public String getType() {
            return type;
        }

        public String getMode() {
            return mode;
        }

        public long getExpiryDate() {
            return expiryDate;
        }

        public LicenseStatus getStatus() {
            return status;
        }

        private static final ConstructingObjectParser<LicenseInfo, Void> PARSER = new ConstructingObjectParser<>(
                "license_info", true, (a, v) -> {
                    String uid = (String) a[0];
                    String type = (String) a[1];
                    String mode = (String) a[2];
                    LicenseStatus status = LicenseStatus.fromString((String) a[3]);
                    Long expiryDate = (Long) a[4];
                    long primitiveExpiryDate = expiryDate == null ? BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS : expiryDate;
                    return new LicenseInfo(uid, type, mode, status, primitiveExpiryDate);
                });
        static {
            PARSER.declareString(constructorArg(), new ParseField("uid"));
            PARSER.declareString(constructorArg(), new ParseField("type"));
            PARSER.declareString(constructorArg(), new ParseField("mode"));
            PARSER.declareString(constructorArg(), new ParseField("status"));
            PARSER.declareLong(optionalConstructorArg(), new ParseField("expiry_date_in_millis"));
        }
    }

    public static class BuildInfo {
        private final String hash;
        private final String timestamp;

        public BuildInfo(String hash, String timestamp) {
            this.hash = hash;
            this.timestamp = timestamp;
        }

        public String getHash() {
            return hash;
        }

        public String getTimestamp() {
            return timestamp;
        }

        private static final ConstructingObjectParser<BuildInfo, Void> PARSER = new ConstructingObjectParser<>(
                "build_info", true, (a, v) -> new BuildInfo((String) a[0], (String) a[1]));
        static {
            PARSER.declareString(constructorArg(), new ParseField("hash"));
            PARSER.declareString(constructorArg(), new ParseField("date"));
        }
    }

    public static class FeatureSetsInfo {
        private final Map<String, FeatureSet> featureSets;

        public FeatureSetsInfo(Set<FeatureSet> featureSets) {
            Map<String, FeatureSet> map = new HashMap<>(featureSets.size());
            for (FeatureSet featureSet : featureSets) {
                map.put(featureSet.name, featureSet);
            }
            this.featureSets = Collections.unmodifiableMap(map);
        }

        public Map<String, FeatureSet> getFeatureSets() {
            return featureSets;
        }

        public static class FeatureSet {
            private final String name;
            @Nullable private final String description;
            private final boolean available;
            private final boolean enabled;
            @Nullable private final Map<String, Object> nativeCodeInfo;

            public FeatureSet(String name, @Nullable String description, boolean available, boolean enabled,
                              @Nullable Map<String, Object> nativeCodeInfo) {
                this.name = name;
                this.description = description;
                this.available = available;
                this.enabled = enabled;
                this.nativeCodeInfo = nativeCodeInfo;
            }

            public String name() {
                return name;
            }

            @Nullable
            public String description() {
                return description;
            }

            public boolean available() {
                return available;
            }

            public boolean enabled() {
                return enabled;
            }

            @Nullable
            public Map<String, Object> nativeCodeInfo() {
                return nativeCodeInfo;
            }

            private static final ConstructingObjectParser<FeatureSet, String> PARSER = new ConstructingObjectParser<>(
                    "feature_set", true, (a, name) -> {
                        String description = (String) a[0];
                        boolean available = (Boolean) a[1];
                        boolean enabled = (Boolean) a[2];
                        @SuppressWarnings("unchecked") // Matches up with declaration below
                        Map<String, Object> nativeCodeInfo = (Map<String, Object>) a[3];
                        return new FeatureSet(name, description, available, enabled, nativeCodeInfo);
                    });
            static {
                PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
                PARSER.declareBoolean(constructorArg(), new ParseField("available"));
                PARSER.declareBoolean(constructorArg(), new ParseField("enabled"));
                PARSER.declareObject(optionalConstructorArg(), (p, name) -> p.map(), new ParseField("native_code_info"));
            }
        }
    }
}
