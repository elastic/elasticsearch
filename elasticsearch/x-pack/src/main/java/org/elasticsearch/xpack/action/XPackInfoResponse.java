/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.core.License;
import org.elasticsearch.xpack.XPackBuild;

import java.io.IOException;
import java.util.Locale;

/**
 */
public class XPackInfoResponse extends ActionResponse {

    private BuildInfo buildInfo;
    private @Nullable LicenseInfo licenseInfo;

    public XPackInfoResponse() {}

    public XPackInfoResponse(BuildInfo buildInfo, @Nullable LicenseInfo licenseInfo) {
        this.buildInfo = buildInfo;
        this.licenseInfo = licenseInfo;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        buildInfo.writeTo(out);
        if (licenseInfo != null) {
            out.writeBoolean(true);
            licenseInfo.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.buildInfo = new BuildInfo(in);
        this.licenseInfo = in.readBoolean() ? new LicenseInfo(in) : null;
    }

    public static class LicenseInfo implements ToXContent {

        private final String uid;
        private final String type;
        private final long expiryDate;
        private final License.Status status;

        public LicenseInfo(License license) {
            this(license.uid(), license.type(), license.status(), license.expiryDate());
        }

        public LicenseInfo(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), License.Status.readFrom(in), in.readLong());
        }

        public LicenseInfo(String uid, String type, License.Status status, long expiryDate) {
            this.uid = uid;
            this.type = type;
            this.status = status;
            this.expiryDate = expiryDate;
        }

        public String getUid() {
            return uid;
        }

        public String getType() {
            return type;
        }

        public long getExpiryDate() {
            return expiryDate;
        }

        public License.Status getStatus() {
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field("uid", uid)
                    .field("type", type)
                    .field("mode", License.OperationMode.resolve(type).name().toLowerCase(Locale.ROOT))
                    .field("status", status.label())
                    .dateValueField("expiry_date_in_millis", "expiry_date", expiryDate)
                    .endObject();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(uid);
            out.writeString(type);
            status.writeTo(out);
            out.writeLong(expiryDate);
        }
    }

    public static class BuildInfo implements ToXContent {

        private final String hash;
        private final String timestamp;

        public BuildInfo(XPackBuild build) {
            this(build.hash(), build.timestamp());
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
                    .field("timestamp", timestamp)
                    .endObject();
        }

        public void writeTo(StreamOutput output) throws IOException {
            output.writeString(hash);
            output.writeString(timestamp);
        }
    }
}
