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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class NodeEnrollmentResponse {

    private final String httpCaKeystore;
    private final String transportKeystore;
    private final String clusterName;
    private final List<String> nodesAddresses;

    public NodeEnrollmentResponse( String httpCaKeystore, String transportKeystore, String clusterName, List<String> nodesAddresses){
        this.httpCaKeystore = httpCaKeystore;
        this.transportKeystore = transportKeystore;
        this.clusterName = clusterName;
        this.nodesAddresses = Collections.unmodifiableList(nodesAddresses);
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

    private static final ParseField HTTP_CA_KEYSTORE = new ParseField("http_ca_keystore");
    private static final ParseField TRANSPORT_KEYSTORE = new ParseField("transport_keystore");
    private static final ParseField CLUSTER_NAME = new ParseField("cluster_name");
    private static final ParseField NODES_ADDRESSES = new ParseField("nodes_addresses");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<NodeEnrollmentResponse, Void> PARSER =
        new ConstructingObjectParser<>(NodeEnrollmentResponse.class.getName(), true,
            a -> new NodeEnrollmentResponse((String)a[0], (String) a[1], (String) a[2], (List<String>) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HTTP_CA_KEYSTORE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_KEYSTORE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), CLUSTER_NAME);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), NODES_ADDRESSES);
    }

    public static NodeEnrollmentResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeEnrollmentResponse that = (NodeEnrollmentResponse) o;
        return httpCaKeystore.equals(that.httpCaKeystore) && transportKeystore.equals(that.transportKeystore)
            && clusterName.equals(that.clusterName) && nodesAddresses.equals(that.nodesAddresses);
    }

    @Override public int hashCode() {
        return Objects.hash(httpCaKeystore, transportKeystore, clusterName, nodesAddresses);
    }
}
