/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class XPackInfoResponse extends ActionResponse implements ToXContentObject {
    /**
     * Value of the license's expiration time if it should never expire.
     */
    public static final long BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS = Long.MAX_VALUE - TimeUnit.HOURS.toMillis(24 * 365);
    // TODO move this constant to License.java once we move License.java to the protocol jar

    @Nullable
    private BuildInfo buildInfo;
    @Nullable
    private LicenseInfo licenseInfo;
    @Nullable
    private FeatureSetsInfo featureSetsInfo;

    public XPackInfoResponse(StreamInput in) throws IOException {
        super(in);
        this.buildInfo = in.readOptionalWriteable(BuildInfo::new);
        this.licenseInfo = in.readOptionalWriteable(LicenseInfo::new);
        this.featureSetsInfo = in.readOptionalWriteable(FeatureSetsInfo::new);
    }

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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(buildInfo);
        out.writeOptionalWriteable(licenseInfo);
        out.writeOptionalWriteable(featureSetsInfo);
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
        return Strings.toString(this, true, false);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (buildInfo != null) {
            builder.field("build", buildInfo, params);
        }

        EnumSet<XPackInfoRequest.Category> categories = XPackInfoRequest.Category.toSet(
            Strings.splitStringByCommaToArray(params.param("categories", "_all"))
        );
        if (licenseInfo != null) {
            builder.field("license", licenseInfo, params);
        } else if (categories.contains(XPackInfoRequest.Category.LICENSE)) {
            // if the user requested the license info, and there is no license, we should send
            // back an explicit null value (indicating there is no license). This is different
            // than not adding the license info at all
            builder.nullField("license");
        }

        if (featureSetsInfo != null) {
            builder.field("features", featureSetsInfo, params);
        }

        if (params.paramAsBoolean("human", true)) {
            builder.field("tagline", "You know, for X");
        }

        return builder.endObject();
    }

    public static class LicenseInfo implements ToXContentObject, Writeable {
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

        public LicenseInfo(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readString(), LicenseStatus.readFrom(in), in.readLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(uid);
            out.writeString(type);
            out.writeString(mode);
            status.writeTo(out);
            out.writeLong(expiryDate);
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("uid", uid);

            if (builder.getRestApiVersion() == RestApiVersion.V_7 && params.paramAsBoolean("accept_enterprise", false) == false) {
                if (License.LicenseType.ENTERPRISE.getTypeName().equals(type)) {
                    builder.field("type", License.LicenseType.PLATINUM.getTypeName());
                } else {
                    builder.field("type", type);
                }

                if (License.OperationMode.ENTERPRISE.description().equals(mode)) {
                    builder.field("mode", License.OperationMode.PLATINUM.description());
                } else {
                    builder.field("mode", mode);
                }
            } else {
                builder.field("type", type);
                builder.field("mode", mode);
            }

            builder.field("status", status.label());
            if (expiryDate != BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS) {
                builder.timeField("expiry_date_in_millis", "expiry_date", expiryDate);
            }
            return builder.endObject();
        }
    }

    public static class BuildInfo implements ToXContentObject, Writeable {
        private final String hash;
        private final String timestamp;

        public BuildInfo(String hash, String timestamp) {
            this.hash = hash;
            this.timestamp = timestamp;
        }

        public BuildInfo(StreamInput input) throws IOException {
            this(input.readString(), input.readString());
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            output.writeString(hash);
            output.writeString(timestamp);
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
            return Objects.equals(hash, rhs.hash) && Objects.equals(timestamp, rhs.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hash, timestamp);
        }

        private static final ConstructingObjectParser<BuildInfo, Void> PARSER = new ConstructingObjectParser<>(
            "build_info",
            true,
            (a, v) -> new BuildInfo((String) a[0], (String) a[1])
        );
        static {
            PARSER.declareString(constructorArg(), new ParseField("hash"));
            PARSER.declareString(constructorArg(), new ParseField("date"));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("hash", hash).field("date", timestamp).endObject();
        }
    }

    public static class FeatureSetsInfo implements ToXContentObject, Writeable {
        private final Map<String, FeatureSet> featureSets;

        public FeatureSetsInfo(Set<FeatureSet> featureSets) {
            Map<String, FeatureSet> map = Maps.newMapWithExpectedSize(featureSets.size());
            for (FeatureSet featureSet : featureSets) {
                map.put(featureSet.name, featureSet);
            }
            this.featureSets = Collections.unmodifiableMap(map);
        }

        public FeatureSetsInfo(StreamInput in) throws IOException {
            int size = in.readVInt();
            Map<String, FeatureSet> featureSets = Maps.newMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                FeatureSet featureSet = new FeatureSet(in);
                featureSets.put(featureSet.name, featureSet);
            }
            this.featureSets = Collections.unmodifiableMap(featureSets);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(featureSets.size());
            for (FeatureSet featureSet : featureSets.values()) {
                featureSet.writeTo(out);
            }
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            List<String> names = new ArrayList<>(this.featureSets.keySet()).stream().sorted().collect(Collectors.toList());
            for (String name : names) {
                builder.field(name, featureSets.get(name), params);
            }
            return builder.endObject();
        }

        public static class FeatureSet implements ToXContentObject, Writeable {
            private final String name;
            private final boolean available;
            private final boolean enabled;

            public FeatureSet(String name, boolean available, boolean enabled) {
                this.name = name;
                this.available = available;
                this.enabled = enabled;
            }

            public FeatureSet(StreamInput in) throws IOException {
                this(in.readString(), readAvailable(in), in.readBoolean());
                if (in.getVersion().before(Version.V_8_0_0)) {
                    in.readMap(); // backcompat reading native code info, but no longer used here
                }
            }

            // this is separated out so that the removed description can be read from the stream on construction
            // TODO: remove this for 8.0
            private static boolean readAvailable(StreamInput in) throws IOException {
                if (in.getVersion().before(Version.V_7_3_0)) {
                    in.readOptionalString();
                }
                return in.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                if (out.getVersion().before(Version.V_7_3_0)) {
                    out.writeOptionalString(null);
                }
                out.writeBoolean(available);
                out.writeBoolean(enabled);
                if (out.getVersion().before(Version.V_8_0_0)) {
                    out.writeGenericMap(Collections.emptyMap());
                }
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

            @Override
            public boolean equals(Object other) {
                if (other == null || other.getClass() != getClass()) return false;
                if (this == other) return true;
                FeatureSet rhs = (FeatureSet) other;
                return Objects.equals(name, rhs.name) && available == rhs.available && enabled == rhs.enabled;
            }

            @Override
            public int hashCode() {
                return Objects.hash(name, available, enabled);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("available", available);
                builder.field("enabled", enabled);
                return builder.endObject();
            }
        }
    }
}
