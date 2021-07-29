/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public final class NodeEnrollmentResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField HTTP_CA_KEY = new ParseField("http_ca_key");
    private static final ParseField HTTP_CA_CERT = new ParseField("http_ca_cert");
    private static final ParseField TRANSPORT_KEY = new ParseField("transport_key");
    private static final ParseField TRANSPORT_CERT = new ParseField("transport_cert");
    private static final ParseField NODES_ADDRESSES = new ParseField("nodes_addresses");

    private final String httpCaKey;
    private final String httpCaCert;
    private final String transportKey;
    private final String transportCert;
    private final List<String> nodesAddresses;

    public NodeEnrollmentResponse(StreamInput in) throws IOException {
        super(in);
        httpCaKey = in.readString();
        httpCaCert = in.readString();
        transportKey = in.readString();
        transportCert = in.readString();
        nodesAddresses = in.readStringList();
    }

    public NodeEnrollmentResponse(String httpCaKey, String httpCaCert, String transportKey, String transportCert,
                                  List<String> nodesAddresses) {
        this.httpCaKey = httpCaKey;
        this.httpCaCert = httpCaCert;
        this.transportKey = transportKey;
        this.transportCert = transportCert;
        this.nodesAddresses = nodesAddresses;
    }

    public String getHttpCaKey() {
        return httpCaKey;
    }

    public String getHttpCaCert() {
        return httpCaCert;
    }

    public String getTransportKey() {
        return transportKey;
    }

    public String getTransportCert() {
        return transportCert;
    }

    public List<String> getNodesAddresses() {
        return nodesAddresses;
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeString(httpCaKey);
        out.writeString(httpCaCert);
        out.writeString(transportKey);
        out.writeString(transportCert);
        out.writeStringCollection(nodesAddresses);
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(HTTP_CA_KEY.getPreferredName(), httpCaKey);
        builder.field(HTTP_CA_CERT.getPreferredName(), httpCaCert);
        builder.field(TRANSPORT_KEY.getPreferredName(), transportKey);
        builder.field(TRANSPORT_CERT.getPreferredName(), transportCert);
        builder.field(NODES_ADDRESSES.getPreferredName(), nodesAddresses);
        return builder.endObject();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeEnrollmentResponse that = (NodeEnrollmentResponse) o;
        return httpCaKey.equals(that.httpCaKey) && httpCaCert.equals(that.httpCaCert) && transportKey.equals(that.transportKey)
            && transportCert.equals(that.transportCert)
            && nodesAddresses.equals(that.nodesAddresses);
    }

    @Override public int hashCode() {
        return Objects.hash(httpCaKey, httpCaCert, transportKey, transportCert, nodesAddresses);
    }
}
