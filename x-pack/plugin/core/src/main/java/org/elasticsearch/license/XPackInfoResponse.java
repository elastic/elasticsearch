/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackBuild;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class XPackInfoResponse extends ActionResponse {

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

    public static class LicenseInfo implements ToXContentObject, Writeable {

        private final String uid;
        private final String type;
        private final String mode;
        private final long expiryDate;
        private final License.Status status;

        public LicenseInfo(License license) {
            this(license.uid(), license.type(), license.operationMode().name().toLowerCase(Locale.ROOT),
                    license.status(), license.expiryDate());
        }

        public LicenseInfo(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readString(), License.Status.readFrom(in), in.readLong());
        }

        public LicenseInfo(String uid, String type, String mode, License.Status status, long expiryDate) {
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

        public License.Status getStatus() {
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
                .field("uid", uid)
                .field("type", type)
                .field("mode", mode)
                .field("status", status.label());
            if (expiryDate != LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS) {
                builder.timeField("expiry_date_in_millis", "expiry_date", expiryDate);
            }
            return builder.endObject();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(uid);
            out.writeString(type);
            out.writeString(mode);
            status.writeTo(out);
            out.writeLong(expiryDate);
        }
    }

    public static class BuildInfo implements ToXContentObject, Writeable {

        private final String hash;
        private final String timestamp;

        public BuildInfo(XPackBuild build) {
            this(build.shortHash(), build.date());
        }

        public BuildInfo(StreamInput input) throws IOException {
            this(input.readString(), input.readString());
        }

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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field("hash", hash)
                    .field("date", timestamp)
                    .endObject();
        }

        public void writeTo(StreamOutput output) throws IOException {
            output.writeString(hash);
            output.writeString(timestamp);
        }
    }

    public static class FeatureSetsInfo implements ToXContentObject, Writeable {

        private final Map<String, FeatureSet> featureSets;

        public FeatureSetsInfo(StreamInput in) throws IOException {
            int size = in.readVInt();
            Map<String, FeatureSet> featureSets = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                FeatureSet featureSet = new FeatureSet(in);
                featureSets.put(featureSet.name, featureSet);
            }
            this.featureSets = Collections.unmodifiableMap(featureSets);
        }

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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            List<String> names = new ArrayList<>(this.featureSets.keySet()).stream().sorted().collect(Collectors.toList());
            for (String name : names) {
                builder.field(name, featureSets.get(name), params);
            }
            return builder.endObject();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(featureSets.size());
            for (FeatureSet featureSet : featureSets.values()) {
                featureSet.writeTo(out);
            }
        }

        public static class FeatureSet implements ToXContentObject, Writeable {

            private final String name;
            @Nullable private final String description;
            private final boolean available;
            private final boolean enabled;
            @Nullable private final Map<String, Object> nativeCodeInfo;

            public FeatureSet(StreamInput in) throws IOException {
                this(in.readString(), in.readOptionalString(), in.readBoolean(), in.readBoolean(),
                        in.getVersion().onOrAfter(Version.V_5_4_0) ? in.readMap() : null);
            }

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

            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                out.writeOptionalString(description);
                out.writeBoolean(available);
                out.writeBoolean(enabled);
                if (out.getVersion().onOrAfter(Version.V_5_4_0)) {
                    out.writeMap(nativeCodeInfo);
                }
            }
        }

    }
}
