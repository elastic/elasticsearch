/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.xpack;

import org.elasticsearch.client.license.LicenseStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    @Override
    public boolean equals(Object other) {
        if (other == null || other.getClass() != getClass()) return false;
        if (this == other) return true;
        XPackInfoResponse rhs = (XPackInfoResponse) other;
        return Objects.equals(buildInfo, rhs.buildInfo)
                && Objects.equals(licenseInfo, rhs.licenseInfo)
                && Objects.equals(featureSetsInfo, rhs.featureSetsInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buildInfo, licenseInfo, featureSetsInfo);
    }

    @Override
    public String toString() {
        return "XPackInfoResponse{" +
            "buildInfo=" + buildInfo +
            ", licenseInfo=" + licenseInfo +
            ", featureSetsInfo=" + featureSetsInfo +
            '}';
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

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != getClass()) return false;
            if (this == other) return true;
            LicenseInfo rhs = (LicenseInfo) other;
            return Objects.equals(uid, rhs.uid)
                    && Objects.equals(type, rhs.type)
                    && Objects.equals(mode, rhs.mode)
                    && Objects.equals(status, rhs.status)
                    && expiryDate == rhs.expiryDate;
        }

        @Override
        public int hashCode() {
            return Objects.hash(uid, type, mode, status, expiryDate);
        }

        @Override
        public String toString() {
            return "LicenseInfo{" +
                "uid='" + uid + '\'' +
                ", type='" + type + '\'' +
                ", mode='" + mode + '\'' +
                ", status=" + status +
                ", expiryDate=" + expiryDate +
                '}';
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

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != getClass()) return false;
            if (this == other) return true;
            BuildInfo rhs = (BuildInfo) other;
            return Objects.equals(hash, rhs.hash)
                    && Objects.equals(timestamp, rhs.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hash, timestamp);
        }

        @Override
        public String toString() {
            return "BuildInfo{" +
                "hash='" + hash + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
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

        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != getClass()) return false;
            if (this == other) return true;
            FeatureSetsInfo rhs = (FeatureSetsInfo) other;
            return Objects.equals(featureSets, rhs.featureSets);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureSets);
        }

        @Override
        public String toString() {
            return "FeatureSetsInfo{" +
                "featureSets=" + featureSets +
                '}';
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

            public boolean available() {
                return available;
            }

            public boolean enabled() {
                return enabled;
            }

            /**
             * Return native code info
             * @deprecated Use ML info api to find native code info
             */
            @Deprecated
            @Nullable
            public Map<String, Object> nativeCodeInfo() {
                return nativeCodeInfo;
            }

            @Override
            public boolean equals(Object other) {
                if (other == null || other.getClass() != getClass()) return false;
                if (this == other) return true;
                FeatureSet rhs = (FeatureSet) other;
                return Objects.equals(name, rhs.name)
                        && Objects.equals(description, rhs.description)
                        && available == rhs.available
                        && enabled == rhs.enabled
                        && Objects.equals(nativeCodeInfo, rhs.nativeCodeInfo);
            }

            @Override
            public int hashCode() {
                return Objects.hash(name, description, available, enabled, nativeCodeInfo);
            }

            @Override
            public String toString() {
                return "FeatureSet{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    ", available=" + available +
                    ", enabled=" + enabled +
                    ", nativeCodeInfo=" + nativeCodeInfo +
                    '}';
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
