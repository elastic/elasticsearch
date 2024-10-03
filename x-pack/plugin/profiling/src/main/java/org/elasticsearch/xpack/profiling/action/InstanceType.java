/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.core.UpdateForV9;
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
        this.provider = provider != null ? provider : "";
        this.region = region != null ? region : "";
        this.name = name != null ? name : "";
    }

    /**
     * Creates a {@link InstanceType} from a {@link Map} of source data provided from profiling-hosts.
     *
     * @param source the source data
     * @return the {@link InstanceType}
     */
    public static InstanceType fromHostSource(Map<String, Object> source) {
        String provider = (String) source.get("cloud.provider");
        if (provider != null) {
            String region = (String) source.get("cloud.region");
            String instanceType = (String) source.get("host.type");
            return new InstanceType(provider, region, instanceType);
        }

        // Check and handle pre-8.14.0 host sources for backwards-compatibility.
        InstanceType instanceType = fromObsoleteHostSource(source);
        if (instanceType != null) {
            return instanceType;
        }

        // Support for configured tags (ECS).
        // Example of tags:
        // "profiling.host.tags": [
        // "cloud_provider:aws",
        // "cloud_environment:qa",
        // "cloud_region:eu-west-1",
        // ],
        String region = null;
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

        return new InstanceType(provider, region, null);
    }

    @UpdateForV9(owner = UpdateForV9.Owner.PROFILING) // remove this method
    private static InstanceType fromObsoleteHostSource(Map<String, Object> source) {
        // Check and handle AWS.
        String region = (String) source.get("ec2.placement.region");
        if (region != null) {
            String instanceType = (String) source.get("ec2.instance_type");
            return new InstanceType("aws", region, instanceType);
        }

        // Check and handle GCP.
        String zone = (String) source.get("gce.instance.zone");
        if (zone != null) {
            // example: "gce.instance.zone": "projects/123456789/zones/europe-west1-b"
            region = zone.substring(zone.lastIndexOf('/') + 1);
            // region consist of the zone's first two tokens
            String[] tokens = region.split("-", 3);
            if (tokens.length > 2) {
                region = tokens[0] + "-" + tokens[1];
            }

            // Support for the instanceType name requires GCP data containing it.
            // These are not publically available, so we don't support it for now.
            return new InstanceType("gcp", region, null);
        }

        // Check and handle Azure.
        // example: "azure.compute.location": "eastus2"
        region = (String) source.get("azure.compute.location");
        if (region != null) {
            // example: "azure.compute.vmsize": "Standard_D2s_v3"
            String instanceType = (String) source.get("azure.compute.vmsize");
            return new InstanceType("azure", region, instanceType);
        }

        return null;
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
        return provider.equals(that.provider) && region.equals(that.region) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, region, name);
    }

    @Override
    public String toString() {
        return "provider '" + name + "' in region '" + region + "'";
    }
}
