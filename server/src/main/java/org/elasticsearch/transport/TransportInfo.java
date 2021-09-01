/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.ReportingService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Booleans.parseBoolean;

public class TransportInfo implements ReportingService.Info {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportInfo.class);

    /** Whether to add hostname to publish host field when serializing. */
    private static final boolean CNAME_IN_PUBLISH_ADDRESS =
            parseBoolean(System.getProperty("es.transport.cname_in_publish_address"), false);

    private final BoundTransportAddress address;
    private Map<String, BoundTransportAddress> profileAddresses;
    private final boolean cnameInPublishAddressProperty;

    public TransportInfo(BoundTransportAddress address, @Nullable Map<String, BoundTransportAddress> profileAddresses) {
        this(address, profileAddresses, CNAME_IN_PUBLISH_ADDRESS);
    }

    public TransportInfo(BoundTransportAddress address, @Nullable Map<String, BoundTransportAddress> profileAddresses,
                         boolean cnameInPublishAddressProperty) {
        this.address = address;
        this.profileAddresses = profileAddresses;
        this.cnameInPublishAddressProperty = cnameInPublishAddressProperty;
    }

    public TransportInfo(StreamInput in) throws IOException {
        address = new BoundTransportAddress(in);
        int size = in.readVInt();
        if (size > 0) {
            profileAddresses = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                BoundTransportAddress value = new BoundTransportAddress(in);
                profileAddresses.put(key, value);
            }
        }
        this.cnameInPublishAddressProperty = CNAME_IN_PUBLISH_ADDRESS;
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
            publishAddressString = hostString + '/' + publishAddress.toString();
            if (cnameInPublishAddressProperty) {
                deprecationLogger.deprecate(DeprecationCategory.SETTINGS, "cname_in_publish_address",
                    "es.transport.cname_in_publish_address system property is deprecated and no longer affects " + propertyName +
                    " formatting. Remove this property to get rid of this deprecation warning.");
            }
        }
        return publishAddressString;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.array(Fields.BOUND_ADDRESS, (Object[]) address.boundAddresses());
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
