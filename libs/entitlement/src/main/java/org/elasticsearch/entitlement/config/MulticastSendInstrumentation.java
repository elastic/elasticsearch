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

/**
 * Instrumentation for {@code MulticastSocket.send(DatagramPacket, byte)}, which was removed in Java 26.
 * This class is overridden by the {@code main26} source set with a no-op implementation.
 */
public class MulticastSendInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);
        builder.on(MulticastSocket.class, rule -> {
            rule.callingVoid(MulticastSocket::send, DatagramPacket.class, Byte.class)
                .enforce(Policies::allNetworkAccess)
                .elseThrowNotEntitled();
        });
    }
}
