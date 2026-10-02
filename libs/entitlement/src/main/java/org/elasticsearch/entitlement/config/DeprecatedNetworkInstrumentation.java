/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.net.DatagramPacket;
import java.net.MulticastSocket;

public class DeprecatedNetworkInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        // This method is defined in a separate config because it has been removed in Java 26+
        builder.on(MulticastSocket.class, rule -> {
            rule.callingVoid(MulticastSocket::send, DatagramPacket.class, Byte.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
        });
    }
}
