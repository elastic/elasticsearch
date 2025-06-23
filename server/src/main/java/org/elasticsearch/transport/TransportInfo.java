/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class TransportInfo implements ReportingService.Info {

    private final BoundTransportAddress address;
    private Map<String, BoundTransportAddress> profileAddresses;

    public TransportInfo(BoundTransportAddress address, @Nullable Map<String, BoundTransportAddress> profileAddresses) {
        this.address = address;
        this.profileAddresses = profileAddresses;
    }

    public TransportInfo(StreamInput in) throws IOException {
        address = new BoundTransportAddress(in);
        int size = in.readVInt();
        if (size > 0) {
            profileAddresses = Maps.newMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                BoundTransportAddress value = new BoundTransportAddress(in);
                profileAddresses.put(key, value);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        address.writeTo(out);
        if (profileAddresses != null) {
            out.writeVInt(profileAddresses.size());
        } else {
            out.writeVInt(0);
        }
        if (profileAddresses != null && profileAddresses.size() > 0) {
            for (Map.Entry<String, BoundTransportAddress> entry : profileAddresses.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    static final class Fields {
        static final String TRANSPORT = "transport";
        static final String BOUND_ADDRESS = "bound_address";
        static final String PUBLISH_ADDRESS = "publish_address";
        static final String PROFILES = "profiles";
    }

    private String formatPublishAddressString(String propertyName, TransportAddress publishAddress) {
        String publishAddressString = publishAddress.toString();
        String hostString = publishAddress.address().getHostString();
        if (InetAddresses.isInetAddress(hostString) == false) {
            publishAddressString = hostString + '/' + publishAddress;
        }
        return publishAddressString;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.xContentList(Fields.BOUND_ADDRESS, address.boundAddresses());
        builder.field(Fields.PUBLISH_ADDRESS, formatPublishAddressString("transport.publish_address", address.publishAddress()));
        builder.startObject(Fields.PROFILES);
        if (profileAddresses != null && profileAddresses.size() > 0) {
            for (Map.Entry<String, BoundTransportAddress> entry : profileAddresses.entrySet()) {
                builder.startObject(entry.getKey());
                builder.array(Fields.BOUND_ADDRESS, (Object[]) entry.getValue().boundAddresses());
                String propertyName = "transport." + entry.getKey() + ".publish_address";
                builder.field(Fields.PUBLISH_ADDRESS, formatPublishAddressString(propertyName, entry.getValue().publishAddress()));
                builder.endObject();
            }
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public BoundTransportAddress address() {
        return address;
    }

    public BoundTransportAddress getAddress() {
        return address();
    }

    public Map<String, BoundTransportAddress> getProfileAddresses() {
        return profileAddresses();
    }

    public Map<String, BoundTransportAddress> profileAddresses() {
        return profileAddresses;
    }
}
