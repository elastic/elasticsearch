/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        private final int expectedPortCount;

        private boolean resolvedHosts;

        AssertingHostsResolver(int expectedPortCount, String... expectedHosts) {
            this.expectedPortCount = expectedPortCount;
            this.expectedHosts = Sets.newHashSet(expectedHosts);
        }

        @Override
        public List<TransportAddress> resolveHosts(List<String> hosts, int limitPortCounts) {
            assertEquals(expectedPortCount, limitPortCounts);
            assertEquals(expectedHosts, Sets.newHashSet(hosts));
            resolvedHosts = true;
            return emptyList();
        }

        boolean getResolvedHosts() {
            return resolvedHosts;
        }
    }

    public void testScansPortsByDefault() {
        final AssertingHostsResolver hostsResolver = new AssertingHostsResolver(5, "::1", "127.0.0.1");
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getLocalAddresses()).thenReturn(Arrays.asList("::1", "127.0.0.1"));
        new SettingsBasedSeedHostsProvider(Settings.EMPTY, transportService).getSeedAddresses(hostsResolver);
        assertTrue(hostsResolver.getResolvedHosts());
    }

    public void testGetsHostsFromSetting() {
        final AssertingHostsResolver hostsResolver = new AssertingHostsResolver(1, "bar", "foo");
        new SettingsBasedSeedHostsProvider(Settings.builder()
            .putList(SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING.getKey(), "foo", "bar")
            .build(), null).getSeedAddresses(hostsResolver);
        assertTrue(hostsResolver.getResolvedHosts());
    }
}
