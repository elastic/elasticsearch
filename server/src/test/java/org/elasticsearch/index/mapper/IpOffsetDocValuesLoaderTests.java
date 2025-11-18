/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.network.NetworkAddress;

public class IpOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {

    public void testOffsetArray() throws Exception {
        verifyOffsets("{\"field\":[\"192.168.1.1\",\"192.168.1.3\",\"192.168.1.2\",\"192.168.1.1\",\"192.168.1.9\",\"192.168.1.3\"]}");
        verifyOffsets("{\"field\":[\"192.168.1.4\",null,\"192.168.1.3\",\"192.168.1.2\",null,\"192.168.1.1\"]}");
    }

    public void testOffsetNestedArray() throws Exception {
        verifyOffsets(
            "{\"field\":[\"192.168.1.2\",[\"192.168.1.1\"],[\"192.168.1.0\"],null,\"192.168.1.0\"]}",
            "{\"field\":[\"192.168.1.2\",\"192.168.1.1\",\"192.168.1.0\",null,\"192.168.1.0\"]}"
        );
        verifyOffsets(
            "{\"field\":[\"192.168.1.6\",[\"192.168.1.5\", [\"192.168.1.4\"]],[\"192.168.1.3\", [\"192.168.1.2\"]],null,\"192.168.1.1\"]}",
            "{\"field\":[\"192.168.1.6\",\"192.168.1.5\",\"192.168.1.4\",\"192.168.1.3\",\"192.168.1.2\",null,\"192.168.1.1\"]}"
        );
    }

    @Override
    protected String getFieldTypeName() {
        return "ip";
    }

    @Override
    protected Object randomValue() {
        return NetworkAddress.format(randomIp(true));
    }
}
