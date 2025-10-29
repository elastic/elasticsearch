/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

public class IpFallbackSyntheticSourceReader extends FallbackSyntheticSourceBlockLoader.SingleValueReader<InetAddress> {

    public IpFallbackSyntheticSourceReader(Object nullValue) {
        super(nullValue);
    }

    @Override
    public void convertValue(Object value, List<InetAddress> accumulator) {
        try {
            if (value instanceof InetAddress ia) {
                accumulator.add(ia);
            } else {
                InetAddress address = InetAddresses.forString(value.toString());
                accumulator.add(address);
            }
        } catch (Exception e) {
            // value is malformed, skip it
        }
    }

    @Override
    public void writeToBlock(List<InetAddress> values, BlockLoader.Builder blockBuilder) {
        BlockLoader.BytesRefBuilder builder = (BlockLoader.BytesRefBuilder) blockBuilder;
        for (InetAddress address : values) {
            var bytesRef = new BytesRef(InetAddressPoint.encode(address));
            builder.appendBytesRef(bytesRef);
        }
    }

    @Override
    protected void parseNonNullValue(XContentParser parser, List<InetAddress> accumulator) throws IOException {
        try {
            InetAddress address = InetAddresses.forString(parser.text());
            accumulator.add(address);
        } catch (Exception e) {
            // value is malformed, skip it
        }
    }

}
