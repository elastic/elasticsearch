/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.SeedHostsProvider.HostsResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SettingsBasedSeedHostsProviderTests extends ESTestCase {

    private class AssertingHostsResolver implements HostsResolver {
        private final Set<String> expectedHosts;

        private boolean resolvedHosts;

        AssertingHostsResolver(String... expectedHosts) {
            this.expectedHosts = Sets.newHashSet(expectedHosts);
        }

        @Override
        public List<TransportAddress> resolveHosts(List<String> hosts) {
            assertEquals(expectedHosts, Sets.newHashSet(hosts));
            resolvedHosts = true;
            return emptyList();
        }

        boolean getResolvedHosts() {
            return resolvedHosts;
        }
    }

    public void testScansPortsByDefault() {
        final AssertingHostsResolver hostsResolver = new AssertingHostsResolver(
            "[::1]:9300", "[::1]:9301", "127.0.0.1:9300", "127.0.0.1:9301"
        );
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getDefaultSeedAddresses()).thenReturn(
            Arrays.asList("[::1]:9300", "[::1]:9301", "127.0.0.1:9300", "127.0.0.1:9301")
        );
        new SettingsBasedSeedHostsProvider(Settings.EMPTY, transportService).getSeedAddresses(hostsResolver);
        assertTrue(hostsResolver.getResolvedHosts());
    }

    public void testGetsHostsFromSetting() {
        final AssertingHostsResolver hostsResolver = new AssertingHostsResolver("bar", "foo");
        new SettingsBasedSeedHostsProvider(Settings.builder()
            .putList(SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING.getKey(), "foo", "bar")
            .build(), null).getSeedAddresses(hostsResolver);
        assertTrue(hostsResolver.getResolvedHosts());
    }
}
