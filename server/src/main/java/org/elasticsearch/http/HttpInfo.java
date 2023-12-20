/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class HttpInfo implements ReportingService.Info {

    private final BoundTransportAddress address;
    private final long maxContentLength;

    public HttpInfo(StreamInput in) throws IOException {
        this(new BoundTransportAddress(in), in.readLong());
    }

    public HttpInfo(BoundTransportAddress address, long maxContentLength) {
        this.address = address;
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        address.writeTo(out);
        out.writeLong(maxContentLength);
    }

    static final class Fields {
        static final String HTTP = "http";
        static final String BOUND_ADDRESS = "bound_address";
        static final String PUBLISH_ADDRESS = "publish_address";
        static final String MAX_CONTENT_LENGTH = "max_content_length";
        static final String MAX_CONTENT_LENGTH_IN_BYTES = "max_content_length_in_bytes";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HTTP);
        builder.array(Fields.BOUND_ADDRESS, (Object[]) address.boundAddresses());
        TransportAddress publishAddress = address.publishAddress();
        String publishAddressString = publishAddress.toString();
        String hostString = publishAddress.address().getHostString();
        if (InetAddresses.isInetAddress(hostString) == false) {
            publishAddressString = hostString + '/' + publishAddress;
        }
        builder.field(Fields.PUBLISH_ADDRESS, publishAddressString);
        builder.humanReadableField(Fields.MAX_CONTENT_LENGTH_IN_BYTES, Fields.MAX_CONTENT_LENGTH, ByteSizeValue.ofBytes(maxContentLength));
        builder.endObject();
        return builder;
    }

    public BoundTransportAddress address() {
        return address;
    }

    public BoundTransportAddress getAddress() {
        return address();
    }

}
