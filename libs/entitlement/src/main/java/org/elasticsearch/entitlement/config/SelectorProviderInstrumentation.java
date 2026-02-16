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

import java.net.ProtocolFamily;
import java.nio.channels.spi.SelectorProvider;

public class SelectorProviderInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(SelectorProvider.provider().getClass(), rule -> {
            rule.calling(SelectorProvider::inheritedChannel).enforce(Policies::changeNetworkHandling).elseThrowNotEntitled();
            rule.calling(SelectorProvider::openDatagramChannel).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(SelectorProvider::openDatagramChannel, ProtocolFamily.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(SelectorProvider::openServerSocketChannel).enforce(Policies::inboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(SelectorProvider::openServerSocketChannel, ProtocolFamily.class)
                .enforce(Policies::inboundNetworkAccess)
                .elseThrowNotEntitled();
            rule.calling(SelectorProvider::openSocketChannel).enforce(Policies::outboundNetworkAccess).elseThrowNotEntitled();
            rule.calling(SelectorProvider::openSocketChannel, ProtocolFamily.class)
                .enforce(Policies::outboundNetworkAccess)
                .elseThrowNotEntitled();
        });
    }
}
