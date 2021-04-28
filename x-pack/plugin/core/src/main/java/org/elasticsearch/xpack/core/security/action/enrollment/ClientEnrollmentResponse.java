/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public final class ClientEnrollmentResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField HTTP_CA = new ParseField("http_ca");
    private static final ParseField NODES_ADDRESSES = new ParseField("nodes_addresses");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ClientEnrollmentResponse, Void> PARSER =
        new ConstructingObjectParser<>(ClientEnrollmentResponse.class.getName(), true,
            a -> new ClientEnrollmentResponse((String) a[0], (List<String>) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), NODES_ADDRESSES);
    }

    private final String httpCa;
    private final List<String> nodesAddresses;

    public ClientEnrollmentResponse(StreamInput in) throws IOException {
        super(in);
        httpCa = in.readString();
        nodesAddresses = in.readStringList();
    }

    public ClientEnrollmentResponse(String httpCa, List<String> nodesAddresses) {
        this.httpCa = httpCa;
        this.nodesAddresses = nodesAddresses;
    }

    public String getHttpCa() {
        return httpCa;
    }

    public List<String> getNodesAddresses() {
        return nodesAddresses;
    }

    @Override public XContentBuilder toXContent(
        XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(HTTP_CA.getPreferredName(), httpCa);
        builder.field(NODES_ADDRESSES.getPreferredName(), nodesAddresses);
        return builder.endObject();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeString(httpCa);
        out.writeStringCollection(nodesAddresses);
    }

    public static ClientEnrollmentResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientEnrollmentResponse response = (ClientEnrollmentResponse) o;
        return httpCa.equals(response.httpCa) && nodesAddresses.equals(response.nodesAddresses);
    }

    @Override public int hashCode() {
        return Objects.hash(httpCa, nodesAddresses);
    }
}
