/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.xpack;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public final class ClientEnrollmentResponse {

    private String httpCa;
    private List<String> nodesAddresses;

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

    public static ClientEnrollmentResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientEnrollmentResponse that = (ClientEnrollmentResponse) o;
        return httpCa.equals(that.httpCa) && nodesAddresses.equals(that.nodesAddresses);
    }

    @Override public int hashCode() {
        return Objects.hash(httpCa, nodesAddresses);
    }
}
