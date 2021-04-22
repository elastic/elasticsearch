/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public final class NodeEnrollmentResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField HTTP_CA_KEYSTORE = new ParseField("http_ca_keystore");
    private static final ParseField TRANSPORT_KEYSTORE = new ParseField("transport_keystore");
    private static final ParseField CLUSTER_NAME = new ParseField("cluster_name");
    private static final ParseField NODES_ADDRESSES = new ParseField("nodes_addresses");

    private final String httpCaKeystore;
    private final String transportKeystore;
    private final String clusterName;
    private final List<String> nodesAddresses;

    public NodeEnrollmentResponse(StreamInput in) throws IOException {
        super(in);
        httpCaKeystore = in.readString();
        transportKeystore = in.readString();
        clusterName = in.readString();
        nodesAddresses = in.readStringList();
    }

    public NodeEnrollmentResponse(String httpKeystore, String transportKeystore, String clusterName, List<String> nodesAddresses) {
        this.httpCaKeystore = httpKeystore;
        this.transportKeystore = transportKeystore;
        this.clusterName = clusterName;
        this.nodesAddresses = nodesAddresses;
    }

    public String getHttpCaKeystore() {
        return httpCaKeystore;
    }

    public String getTransportKeystore() {
        return transportKeystore;
    }

    public String getClusterName() {
        return clusterName;
    }

    public List<String> getNodesAddresses() {
        return nodesAddresses;
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeString(httpCaKeystore);
        out.writeString(transportKeystore);
        out.writeString(clusterName);
        out.writeStringCollection(nodesAddresses);
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(HTTP_CA_KEYSTORE.getPreferredName(), httpCaKeystore);
        builder.field(TRANSPORT_KEYSTORE.getPreferredName(), transportKeystore);
        builder.field(CLUSTER_NAME.getPreferredName(), clusterName);
        builder.field(NODES_ADDRESSES.getPreferredName(), nodesAddresses);
        return builder.endObject();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeEnrollmentResponse response = (NodeEnrollmentResponse) o;
        return httpCaKeystore.equals(response.httpCaKeystore) && transportKeystore.equals(response.transportKeystore) && clusterName.equals(
            response.clusterName) && nodesAddresses.equals(response.nodesAddresses);
    }

    @Override public int hashCode() {
        return Objects.hash(httpCaKeystore, transportKeystore, clusterName, nodesAddresses);
    }
}
