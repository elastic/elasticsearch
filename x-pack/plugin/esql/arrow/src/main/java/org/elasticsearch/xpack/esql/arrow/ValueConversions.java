/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;

/**
 * Utilities to convert some of byte-encoded ESQL values into to a format more suitable
 * for Arrow output.
 */
public class ValueConversions {

    /**
     * Shorten ipv6-mapped ipv4 IP addresses to 4 bytes
     */
    public static BytesRef shortenIpV4Addresses(BytesRef value, BytesRef scratch) {
        // Same logic as sun.net.util.IPAddressUtil#isIPv4MappedAddress
        // See https://datatracker.ietf.org/doc/html/rfc4291#section-2.5.5.2
        if (value.length == 16) {
            int pos = value.offset;
            byte[] bytes = value.bytes;
            boolean isIpV4 = bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == 0
                && bytes[pos++] == (byte) 0xFF
                && bytes[pos] == (byte) 0xFF;

            if (isIpV4) {
                scratch.bytes = value.bytes;
                scratch.offset = value.offset + 12;
                scratch.length = 4;
                return scratch;
            }
        }
        return value;
    }

    /**
     * Convert binary-encoded versions to strings
     */
    public static BytesRef versionToString(BytesRef value, BytesRef scratch) {
        return new BytesRef(new Version(value).toString());
    }

    /**
     * Convert any xcontent source to json
     */
    public static BytesRef sourceToJson(BytesRef value, BytesRef scratch) {
        try {
            var valueArray = new BytesArray(value);
            XContentType xContentType = XContentHelper.xContentType(valueArray);
            if (xContentType == XContentType.JSON) {
                return value;
            } else {
                String json = XContentHelper.convertToJson(valueArray, false, xContentType);
                return new BytesRef(json);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
