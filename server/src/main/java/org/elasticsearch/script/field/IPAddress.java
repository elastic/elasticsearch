/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;

/**
 * IP address for use in scripting.
 */
public class IPAddress implements ToXContent {
    protected final InetAddress address;

    IPAddress(InetAddress address) {
        this.address = address;
    }

    public IPAddress(String address) {
        this.address = InetAddresses.forString(address);
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
    public boolean isFragment() {
        return false;
    }
}
