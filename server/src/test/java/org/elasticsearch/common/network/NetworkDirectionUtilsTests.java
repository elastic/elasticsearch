/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.network;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class NetworkDirectionUtilsTests extends ESTestCase {
    public void testCIDR() {
        testNetworkDirectionUtils("10.0.1.1", "192.168.1.2", List.of("10.0.0.0/8"), "outbound");
        testNetworkDirectionUtils("192.168.1.2", "10.0.1.1", List.of("10.0.0.0/8"), "inbound");
    }

    public void testUnspecified() {
        testNetworkDirectionUtils("0.0.0.0", "0.0.0.0", List.of("unspecified"), "internal");
        testNetworkDirectionUtils("::", "::", List.of("unspecified"), "internal");
    }

    public void testNetworkPrivate() {
        testNetworkDirectionUtils("192.168.1.1", "192.168.1.2", List.of("private"), "internal");
        testNetworkDirectionUtils("10.0.1.1", "192.168.1.2", List.of("private"), "internal");
        testNetworkDirectionUtils("192.168.1.1", "172.16.0.1", List.of("private"), "internal");
        testNetworkDirectionUtils("192.168.1.1", "fd12:3456:789a:1::1", List.of("private"), "internal");
    }

    public void testNetworkPublic() {
        testNetworkDirectionUtils("192.168.1.1", "192.168.1.2", List.of("public"), "external");
        testNetworkDirectionUtils("10.0.1.1", "192.168.1.2", List.of("public"), "external");
        testNetworkDirectionUtils("192.168.1.1", "172.16.0.1", List.of("public"), "external");
        testNetworkDirectionUtils("192.168.1.1", "fd12:3456:789a:1::1", List.of("public"), "external");
    }

    private void testNetworkDirectionUtils(String source, String destination, List<String> networks, String expectedDirection) {
        boolean sourceInternal = NetworkDirectionUtils.isInternal(networks, source);
        boolean destinationInternal = NetworkDirectionUtils.isInternal(networks, destination);
        assertThat(expectedDirection, equalTo(NetworkDirectionUtils.getDirection(sourceInternal, destinationInternal)));
    }
}
