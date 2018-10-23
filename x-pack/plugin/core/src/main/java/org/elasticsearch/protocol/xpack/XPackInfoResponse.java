/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class XPackInfoResponse extends ActionResponse implements ToXContentObject {
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
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(buildInfo);
        out.writeOptionalWriteable(licenseInfo);
        out.writeOptionalWriteable(featureSetsInfo);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.buildInfo = in.readOptionalWriteable(BuildInfo::new);
        this.licenseInfo = in.readOptionalWriteable(LicenseInfo::new);
        this.featureSetsInfo = in.readOptionalWriteable(FeatureSetsInfo::new);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (buildInfo != null) {
            builder.field("build", buildInfo, params);
        }

        EnumSet<XPackInfoRequest.Category> categories = XPackInfoRequest.Category
                .toSet(Strings.splitStringByCommaToArray(params.param("categories", "_all")));
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
                .field("uid", uid)
                .field("type", type)
                .field("mode", mode)
                .field("status", status.label());
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
            return Objects.equals(hash, rhs.hash)
                    && Objects.equals(timestamp, rhs.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hash, timestamp);
        }

        private static final ConstructingObjectParser<BuildInfo, Void> PARSER = new ConstructingObjectParser<>(
                "build_info", true, (a, v) -> new BuildInfo((String) a[0], (String) a[1]));
        static {
            PARSER.declareString(constructorArg(), new ParseField("hash"));
            PARSER.declareString(constructorArg(), new ParseField("date"));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field("hash", hash)
                    .field("date", timestamp)
                    .endObject();
        }
    }

    public static class FeatureSetsInfo implements ToXContentObject, Writeable {
        private final Map<String, FeatureSet> featureSets;

        public FeatureSetsInfo(Set<FeatureSet> featureSets) {
            Map<String, FeatureSet> map = new HashMap<>(featureSets.size());
            for (FeatureSet featureSet : featureSets) {
                map.put(featureSet.name, featureSet);
            }
            this.featureSets = Collections.unmodifiableMap(map);
        }

        public FeatureSetsInfo(StreamInput in) throws IOException {
            int size = in.readVInt();
            Map<String, FeatureSet> featureSets = new HashMap<>(size);
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

            public FeatureSet(StreamInput in) throws IOException {
                this(in.readString(), in.readOptionalString(), in.readBoolean(), in.readBoolean(), in.readMap());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                out.writeOptionalString(description);
                out.writeBoolean(available);
                out.writeBoolean(enabled);
                out.writeMap(nativeCodeInfo);
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

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                if (description != null) {
                    builder.field("description", description);
                }
                builder.field("available", available);
                builder.field("enabled", enabled);
                if (nativeCodeInfo != null) {
                    builder.field("native_code_info", nativeCodeInfo);
                }
                return builder.endObject();
            }
        }
    }
}
