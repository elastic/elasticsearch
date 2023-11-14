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

    public static DatacenterInstance fromSource(Map<String, Object> source) {
        return new DatacenterInstance((String) source.get("provider"), (String) source.get("region"), (String) source.get("instance_type"));
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
