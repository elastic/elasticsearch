/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class RemoteClusterServerInfo implements ReportingService.Info {

    private final BoundTransportAddress address;

    public RemoteClusterServerInfo(StreamInput in) throws IOException {
        this(new BoundTransportAddress(in));
    }

    public RemoteClusterServerInfo(BoundTransportAddress address) {
        this.address = address;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("remote_cluster_server");
        builder.array("bound_address", (Object[]) address.boundAddresses());
        TransportAddress publishAddress = address.publishAddress();
        String publishAddressString = publishAddress.toString();
        String hostString = publishAddress.address().getHostString();
        if (InetAddresses.isInetAddress(hostString) == false) {
            publishAddressString = hostString + '/' + publishAddress;
        }
        builder.field("publish_address", publishAddressString);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        address.writeTo(out);
    }

    public BoundTransportAddress getAddress() {
        return address;
    }
}
