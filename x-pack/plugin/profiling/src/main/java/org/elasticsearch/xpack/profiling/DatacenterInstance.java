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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class DatacenterInstance implements ToXContentObject {
    String provider;
    String region;
    String instanceType;

    DatacenterInstance(String provider, String region, String instanceType) {
        this.provider = provider;
        this.region = region;
        this.instanceType = instanceType;
    }

    /**
     * Creates a {@link DatacenterInstance} from a {@link Map} of source data provided from JSON or profiling-costs.
     *
     * @param source the source data
     * @return the {@link DatacenterInstance}
     */
    public static DatacenterInstance fromCostSource(Map<String, Object> source) {
        return new DatacenterInstance((String) source.get("provider"), (String) source.get("region"), (String) source.get("instance_type"));
    }

    /**
     * Creates a {@link DatacenterInstance} from a {@link Map} of source data provided from profiling-hosts.
     *
     * @param source the source data
     * @return the {@link DatacenterInstance}
     */
    public static DatacenterInstance fromHostSource(Map<String, Object> source) {
        // Example of tags:
        // "profiling.host.tags": [
        // "cloud_provider:aws",
        // "cloud_environment:qa",
        // "cloud_region:eu-west-1",
        // ],
        String provider = null, region = null, instanceType = null;

        String[] tags = (String[]) source.get("profiling.host.tags");
        for (String tag : tags) {
            String[] kv = tag.toLowerCase(Locale.ROOT).split(":", 2);
            if (Objects.equals(kv[0], "cloud_provider")) {
                provider = kv[1];
            }
            if (Objects.equals(kv[0], "cloud_region")) {
                region = kv[1];
            }
        }

        // We only support AWS for 8.12, but plan for GCP and Azure later.
        switch (Objects.requireNonNull(provider)) {
            case "aws":
                instanceType = (String) source.get("ec2.instance_type");
                break;
            case "gcp":
                // check 'gce.instance.name' or 'gce.instance.name' to extract the instanceType
            case "azure":
                // extract the instanceType
        }

        return new DatacenterInstance(provider, region, instanceType);
    }

    public boolean isEmpty() {
        return provider == null && region == null && instanceType == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("provider", this.provider);
        builder.field("region", this.region);
        builder.field("instance_type", this.instanceType);
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
        DatacenterInstance that = (DatacenterInstance) o;
        return Objects.equals(provider, that.provider)
            && Objects.equals(region, that.region)
            && Objects.equals(instanceType, that.instanceType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, region, instanceType);
    }
}
