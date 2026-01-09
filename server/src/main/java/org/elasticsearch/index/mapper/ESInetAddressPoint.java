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
import org.elasticsearch.common.network.NetworkAddress;
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

    /**
     * Creates a new ESInetAddressPoint, indexing the provided address.
     * <p>
     * This is the difference compared to {@link #ESInetAddressPoint(String, InetAddress)}
     * and {@link InetAddressPoint#InetAddressPoint(String, InetAddress)}
     * is that this constructor uses a more efficient way to parse the IP address that avoids the need to create
     * a {@link String} and an {@link InetAddress} object for the IP address.
     *
     * @param name the name of the field
     * @param value the IP address as a string
     * @throws IllegalArgumentException if the field name or value is null or if the IP address is invalid
     */
    protected ESInetAddressPoint(String name, XContentString value) {
        super(name, TYPE);
        if (value == null) {
            throw new IllegalArgumentException("point must not be null");
        }
        this.fieldsData = new BytesRef(InetAddresses.encodeAsIpv6(value));
        this.ipString = value;
        this.inetAddress = null;
    }

    /**
     * Creates a new ESInetAddressPoint, indexing the provided address.
     * <p>
     * This constructor is similar to Lucene's InetAddressPoint.
     * For performance reasons, it is recommended to use the constructor that accepts
     * an {@link XContentString} representation of the IP address instead.
     *
     * @param name field name
     * @param value InetAddress value
     * @throws IllegalArgumentException if the field name or value is null.
     */
    protected ESInetAddressPoint(String name, InetAddress value) {
        super(name, TYPE);
        if (value == null) {
            throw new IllegalArgumentException("point must not be null");
        }
        this.fieldsData = new BytesRef(CIDRUtils.encode(value.getAddress()));
        this.inetAddress = value;
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
            result.append(NetworkAddress.format(address));
            result.append(']');
        } else {
            result.append(NetworkAddress.format(address));
        }

        result.append('>');
        return result.toString();
    }
}
