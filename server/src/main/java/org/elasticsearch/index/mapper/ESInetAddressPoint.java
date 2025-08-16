/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.CIDRUtils;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentString;

import java.net.InetAddress;

/**
 * A Lucene {@link Field} that stores an IP address as a point.
 * This is similar to {@link InetAddressPoint} but uses a more efficient way to parse IP addresses
 * that doesn't require the address to be an {@link InetAddress} object.
 * Otherwise, it behaves just like the {@link InetAddressPoint} field.
 */
class ESInetAddressPoint extends Field {
    private static final FieldType TYPE;

    static {
        TYPE = new FieldType();
        TYPE.setDimensions(1, InetAddressPoint.BYTES);
        TYPE.freeze();
    }

    private final XContentString ipString;
    private final InetAddress inetAddress;

    protected ESInetAddressPoint(String name, XContentString ipString) {
        super(name, TYPE);
        this.fieldsData = new BytesRef(InetAddresses.encodeAsIpv6(ipString));
        this.ipString = ipString;
        this.inetAddress = null;
    }

    protected ESInetAddressPoint(String name, InetAddress inetAddress) {
        super(name, TYPE);
        this.fieldsData = new BytesRef(CIDRUtils.encode(inetAddress.getAddress()));
        this.inetAddress = inetAddress;
        this.ipString = null;
    }

    public InetAddress getInetAddress() {
        if (ipString != null) {
            return InetAddresses.forString(ipString.bytes());
        }
        if (inetAddress != null) {
            return inetAddress;
        }
        throw new IllegalStateException("Neither ipString nor inetAddress is set");
    }

    @Override
    @SuppressForbidden(
        reason = "Calling InetAddress#getHostAddress to mimic what InetAddressPoint does. "
            + "Some tests depend on the exact string representation."
    )
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(getClass().getSimpleName());
        result.append(" <");
        result.append(name);
        result.append(':');

        // IPv6 addresses are bracketed, to not cause confusion with historic field:value representation
        BytesRef bytes = (BytesRef) fieldsData;
        InetAddress address = InetAddressPoint.decode(BytesRef.deepCopyOf(bytes).bytes);
        if (address.getAddress().length == 16) {
            result.append('[');
            result.append(address.getHostAddress());
            result.append(']');
        } else {
            result.append(address.getHostAddress());
        }

        result.append('>');
        return result.toString();
    }
}
