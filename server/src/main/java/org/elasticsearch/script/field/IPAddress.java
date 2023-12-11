/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;

/**
 * IP address for use in scripting.
 */
public class IPAddress implements ToXContentObject, GenericNamedWriteable {
    static final String NAMED_WRITEABLE_NAME = "IPAddress";
    protected final InetAddress address;

    IPAddress(InetAddress address) {
        this.address = address;
    }

    public IPAddress(String address) {
        this.address = InetAddresses.forString(address);
    }

    public IPAddress(StreamInput input) throws IOException {
        this(input.readString());
    }

    public void writeTo(StreamOutput output) throws IOException {
        output.writeString(toString());
    }

    public boolean isV4() {
        return address instanceof Inet4Address;
    }

    public boolean isV6() {
        return address instanceof Inet6Address;
    }

    @Override
    public String toString() {
        return InetAddresses.toAddrString(address);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(this.toString());
    }

    @Override
    public String getWriteableName() {
        return NAMED_WRITEABLE_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.IP_ADDRESS_WRITEABLE;
    }
}
