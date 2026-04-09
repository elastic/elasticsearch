/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.Scope;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class ElasticsearchTests extends ESTestCase {
    public void testFindPluginsWithNativeAccess() {

        var policies = Map.ofEntries(
            entry(
                "plugin-with-native",
                new Policy(
                    "policy",
                    List.of(
                        new Scope("module.a", List.of(new LoadNativeLibrariesEntitlement())),
                        new Scope("module.b", List.of(new InboundNetworkEntitlement()))
                    )
                )
            ),
            entry(
                "another-plugin-with-native",
                new Policy(
                    "policy",
                    List.of(
                        new Scope("module.a2", List.of(new LoadNativeLibrariesEntitlement())),
                        new Scope("module.b2", List.of(new LoadNativeLibrariesEntitlement())),
                        new Scope("module.c2", List.of(new InboundNetworkEntitlement()))

                    )
                )
            ),
            entry(
                "plugin-without-native",
                new Policy(
                    "policy",
                    List.of(
                        new Scope("module.a3", List.of(new InboundNetworkEntitlement())),
                        new Scope("module.b3", List.of(new OutboundNetworkEntitlement()))
                    )
                )
            )
        );

        var pluginsWithNativeAccess = Elasticsearch.findPluginsWithNativeAccess(policies);

        assertThat(pluginsWithNativeAccess.keySet(), containsInAnyOrder("plugin-with-native", "another-plugin-with-native"));
        assertThat(pluginsWithNativeAccess.get("plugin-with-native"), containsInAnyOrder("module.a"));
        assertThat(pluginsWithNativeAccess.get("another-plugin-with-native"), containsInAnyOrder("module.a2", "module.b2"));
    }
}
