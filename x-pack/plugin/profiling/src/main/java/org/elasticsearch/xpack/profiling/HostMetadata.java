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
    String hostID;
    DatacenterInstance dci;

    HostMetadata(String hostID, DatacenterInstance dci) {
        this.hostID = hostID;
        this.dci = dci;
    }

    public static HostMetadata fromSource(Map<String, Object> source) {
        if (source != null) {
            String hostID = (String) source.get("host.id");
            return new HostMetadata(hostID, DatacenterInstance.fromHostSource(source));
        }
        return null;
    }

    public boolean isEmpty() {
        return hostID == null || dci == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        dci.toXContent(builder, params);
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
