/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class InstanceType implements ToXContentObject {
    final String provider;
    final String region;
    final String name;

    InstanceType(String provider, String region, String name) {
        this.provider = provider;
        this.region = region;
        this.name = name;
    }

    /**
     * Creates a {@link InstanceType} from a {@link Map} of source data provided from JSON or profiling-costs.
     *
     * @param source the source data
     * @return the {@link InstanceType}
     */
    public static InstanceType fromCostSource(Map<String, Object> source) {
        return new InstanceType((String) source.get("provider"), (String) source.get("region"), (String) source.get("instance_type"));
    }

    /**
     * Creates a {@link InstanceType} from a {@link Map} of source data provided from profiling-hosts.
     *
     * @param source the source data
     * @return the {@link InstanceType}
     */
    public static InstanceType fromHostSource(Map<String, Object> source) {
        // Example of tags:
        // "profiling.host.tags": [
        // "cloud_provider:aws",
        // "cloud_environment:qa",
        // "cloud_region:eu-west-1",
        // ],
        String provider = "";
        String region = "";
        String instanceType = "";

        List<String> tags = listOf(source.get("profiling.host.tags"));
        for (String tag : tags) {
            String[] kv = tag.toLowerCase(Locale.ROOT).split(":", 2);
            if (kv.length != 2) {
                continue;
            }
            if ("cloud_provider".equals(kv[0])) {
                provider = kv[1];
            }
            if ("cloud_region".equals(kv[0])) {
                region = kv[1];
            }
        }

        // We only support AWS for 8.12, but plan for GCP and Azure later.
        // "gcp": check 'gce.instance.name' or 'gce.instance.name' to extract the instanceType
        // "azure": extract the instanceType
        if ("aws".equals(provider)) {
            instanceType = (String) source.get("ec2.instance_type");
        }

        return new InstanceType(provider, region, instanceType);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> listOf(Object o) {
        if (o instanceof List) {
            return (List<T>) o;
        } else if (o != null) {
            return List.of((T) o);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("provider", this.provider);
        builder.field("region", this.region);
        builder.field("instance_type", this.name);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InstanceType that = (InstanceType) o;
        return Objects.equals(provider, that.provider) && Objects.equals(region, that.region) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, region, name);
    }

    @Override
    public String toString() {
        return name + " in region " + region;
    }
}
