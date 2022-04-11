/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class NodeEnrollmentResponse {

    private final String httpCaKey;
    private final String httpCaCert;
    private final String transportCaCert;
    private final String transportKey;
    private final String transportCert;
    private final List<String> nodesAddresses;

    public NodeEnrollmentResponse(
        String httpCaKey,
        String httpCaCert,
        String transportCaCert,
        String transportKey,
        String transportCert,
        List<String> nodesAddresses
    ) {
        this.httpCaKey = httpCaKey;
        this.httpCaCert = httpCaCert;
        this.transportCaCert = transportCaCert;
        this.transportKey = transportKey;
        this.transportCert = transportCert;
        this.nodesAddresses = Collections.unmodifiableList(nodesAddresses);
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

    public String getTransportCaCert() {
        return transportCaCert;
    }

    public String getTransportCert() {
        return transportCert;
    }

    public List<String> getNodesAddresses() {
        return nodesAddresses;
    }

    private static final ParseField HTTP_CA_KEY = new ParseField("http_ca_key");
    private static final ParseField HTTP_CA_CERT = new ParseField("http_ca_cert");
    private static final ParseField TRANSPORT_CA_CERT = new ParseField("transport_ca_cert");
    private static final ParseField TRANSPORT_KEY = new ParseField("transport_key");
    private static final ParseField TRANSPORT_CERT = new ParseField("transport_cert");
    private static final ParseField NODES_ADDRESSES = new ParseField("nodes_addresses");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodeEnrollmentResponse, Void> PARSER = new ConstructingObjectParser<>(
        NodeEnrollmentResponse.class.getName(),
        true,
        a -> {
            final String httpCaKey = (String) a[0];
            final String httpCaCert = (String) a[1];
            final String transportCaCert = (String) a[2];
            final String transportKey = (String) a[3];
            final String transportCert = (String) a[4];
            final List<String> nodesAddresses = (List<String>) a[5];
            return new NodeEnrollmentResponse(httpCaKey, httpCaCert, transportCaCert, transportKey, transportCert, nodesAddresses);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA_KEY);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA_CERT);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_CA_CERT);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_KEY);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_CERT);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), NODES_ADDRESSES);
    }

    public static NodeEnrollmentResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeEnrollmentResponse that = (NodeEnrollmentResponse) o;
        return httpCaKey.equals(that.httpCaKey)
            && httpCaCert.equals(that.httpCaCert)
            && transportCaCert.equals(that.transportCaCert)
            && transportKey.equals(that.transportKey)
            && transportCert.equals(that.transportCert)
            && nodesAddresses.equals(that.nodesAddresses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(httpCaKey, httpCaCert, transportCaCert, transportKey, transportCert, nodesAddresses);
    }
}
