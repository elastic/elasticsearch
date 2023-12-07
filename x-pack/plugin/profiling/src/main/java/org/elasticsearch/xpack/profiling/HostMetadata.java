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

final class HostMetadata implements ToXContentObject {
    final String hostID;
    final InstanceType instanceType;
    final String profilingHostMachine; // aarch64 or x86_64

    HostMetadata(String hostID, InstanceType instanceType, String profilingHostMachine) {
        this.hostID = hostID;
        this.instanceType = instanceType;
        this.profilingHostMachine = profilingHostMachine;
    }

    public static HostMetadata fromSource(Map<String, Object> source) {
        if (source != null) {
            String hostID = (String) source.get("host.id");
            String profilingHostMachine = (String) source.get("profiling.host.machine");
            return new HostMetadata(hostID, InstanceType.fromHostSource(source), profilingHostMachine);
        }
        return new HostMetadata("", new InstanceType("", "", ""), "");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        instanceType.toXContent(builder, params);
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
        HostMetadata that = (HostMetadata) o;
        return Objects.equals(hostID, that.hostID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostID);
    }
}
