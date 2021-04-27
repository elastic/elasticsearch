/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

public class GetFeatureUsageResponse extends ActionResponse implements ToXContentObject {

    public static class FeatureUsageInfo implements Writeable {
        public final String name;
        public final ZonedDateTime lastUsedTime;
        public final String licenseLevel;

        public FeatureUsageInfo(String name, ZonedDateTime lastUsedTime, String licenseLevel) {
            this.name = name;
            this.lastUsedTime = lastUsedTime;
            this.licenseLevel = licenseLevel;
        }

        public FeatureUsageInfo(StreamInput in) throws IOException {
            this.name = in.readString();
            this.lastUsedTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(in.readLong()), ZoneOffset.UTC);
            this.licenseLevel = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeLong(lastUsedTime.toEpochSecond());
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
            builder.field("name", feature.name);
            builder.field("last_used", feature.lastUsedTime.toString());
            builder.field("license_level", feature.licenseLevel);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
