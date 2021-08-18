/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetFeatureUsageResponse extends ActionResponse implements ToXContentObject {

    public static class FeatureUsageInfo implements Writeable {
        final String family;
        final String name;
        final ZonedDateTime lastUsedTime;
        final String context;
        final String licenseLevel;

        public FeatureUsageInfo(@Nullable String family, String name, ZonedDateTime lastUsedTime,
                                @Nullable String context, String licenseLevel) {
            this.family = family;
            this.name = Objects.requireNonNull(name, "Feature name may not be null");
            this.lastUsedTime = Objects.requireNonNull(lastUsedTime, "Last used time may not be null");
            this.context = context;
            this.licenseLevel = Objects.requireNonNull(licenseLevel, "License level may not be null");
        }

        public FeatureUsageInfo(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
                this.family = in.readOptionalString();
            } else {
                this.family = null;
            }
            this.name = in.readString();
            this.lastUsedTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(in.readLong()), ZoneOffset.UTC);
            if (in.getVersion().onOrAfter(Version.V_7_15_0)) {
                this.context = in.readOptionalString();
            } else {
                this.context = null;
            }
            this.licenseLevel = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
                out.writeOptionalString(this.family);
            }
            out.writeString(name);
            out.writeLong(lastUsedTime.toEpochSecond());
            if (out.getVersion().onOrAfter(Version.V_7_15_0)) {
                out.writeOptionalString(this.context);
            }
            out.writeString(licenseLevel);
        }
    }

    private List<FeatureUsageInfo> features;

    public GetFeatureUsageResponse(List<FeatureUsageInfo> features) {
        this.features = Collections.unmodifiableList(features);
    }

    public GetFeatureUsageResponse(StreamInput in) throws IOException {
        this.features = in.readList(FeatureUsageInfo::new);
    }

    public List<FeatureUsageInfo> getFeatures() {
        return features;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(features);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("features");
        for (FeatureUsageInfo feature : features) {
            builder.startObject();
            builder.field("family", feature.family);
            builder.field("name", feature.name);
            builder.field("context", feature.context);
            builder.field("last_used", feature.lastUsedTime.toString());
            builder.field("license_level", feature.licenseLevel);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
