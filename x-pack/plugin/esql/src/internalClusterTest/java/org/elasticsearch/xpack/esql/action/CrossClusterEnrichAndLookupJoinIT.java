/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterEnrichAndLookupJoinIT extends AbstractEnrichBasedCrossClusterTestCase {

    private static final String IP_LOOKUP_INDEX = "ip_lookup";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        return plugins;
    }

    @Before
    public void setupIpLookup() {
        // Same ip -> value mapping as the "hosts" enrich policy (see setupHostsEnrich), exposed as a
        // lookup-mode index instead. Values are identical across clusters, so ENRICH and LOOKUP JOIN
        // resolve the same "os" and "location" for a given ip, regardless of execution order.
        Map<String, String> ipToLocation = Map.ofEntries(
            Map.entry("192.168.1.2", "Windows"),
            Map.entry("192.168.1.3", "MacOS"),
            Map.entry("192.168.1.4", "Linux"),
            Map.entry("192.168.1.5", "Android"),
            Map.entry("192.168.1.6", "iOS"),
            Map.entry("192.168.1.7", "Windows"),
            Map.entry("192.168.1.8", "MacOS"),
            Map.entry("192.168.1.9", "Linux"),
            Map.entry("192.168.1.10", "Linux"),
            Map.entry("192.168.1.11", "Windows")
        );
        for (String cluster : allClusters()) {
            Client client = client(cluster);
            client.admin()
                .indices()
                .prepareCreate(IP_LOOKUP_INDEX)
                .setSettings(Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP))
                .setMapping("ip", "type=keyword", "location", "type=keyword")
                .get();
            for (Map.Entry<String, String> e : ipToLocation.entrySet()) {
                client.prepareIndex(IP_LOOKUP_INDEX).setSource("ip", e.getKey(), "location", e.getValue()).get();
            }
            client.admin().indices().prepareRefresh(IP_LOOKUP_INDEX).get();
        }
    }

    // "FROM *:events" (remote clusters only) with the "hosts" policy resolves to this os breakdown -
    // see CrossClusterEnrichIT#testWithHostsPolicy. ip_lookup mirrors "hosts", so location matches os.
    private static final List<List<Object>> EXPECTED_ROWS = List.of(
        List.of(1L, "Android", "Android"),
        List.of(2L, "Linux", "Linux"),
        List.of(4L, "MacOS", "MacOS"),
        List.of(3L, "Windows", "Windows"),
        List.of(1L, "iOS", "iOS"),
        Arrays.asList(2L, null, null)
    );

    public void testLookupJoinAndEnrich() {
        for (var test : List.of(
            // remote and then remote
            new String[] { "LOOKUP JOIN ip_lookup", "ENRICH _remote:hosts" },
            new String[] { "ENRICH _remote:hosts", "LOOKUP JOIN ip_lookup" },
            // remote and then local
            new String[] { "LOOKUP JOIN ip_lookup", "ENRICH _coordinator:hosts" },
            new String[] { "ENRICH _remote:hosts", "LOOKUP JOIN _coordinator:ip_lookup" },
            // local and then local
            new String[] { "LOOKUP JOIN _coordinator:ip_lookup", "ENRICH _coordinator:hosts" },
            new String[] { "ENRICH _coordinator:hosts", "LOOKUP JOIN _coordinator:ip_lookup" }
        )) {
            try (EsqlQueryResponse resp = runQuery(Strings.format("""
                FROM *:events
                | EVAL ip = TO_STR(host)
                | %s ON ip
                | %s ON ip
                | STATS c = COUNT(*) BY os, location
                | SORT os
                """, test[0], test[1]), null)) {
                assertThat(getValuesList(resp), equalTo(EXPECTED_ROWS));
                assertTrue(resp.getExecutionInfo().isCrossClusterSearch());
            }
        }

        for (var test : List.of(
            // local and then remote is not supported
            new String[] {
                "LOOKUP JOIN _coordinator:ip_lookup",
                "ENRICH _remote:hosts",
                "ENRICH with remote policy can't be executed after [LOOKUP JOIN _coordinator:ip_lookup ON ip]" },
            new String[] {
                "ENRICH _coordinator:hosts",
                "LOOKUP JOIN ip_lookup",
                "LOOKUP JOIN with remote indices can't be executed after [ENRICH _coordinator:hosts ON ip]" }
        )) {
            expectThrows(VerificationException.class, containsString(test[2]), () -> runQuery(Strings.format("""
                FROM *:events
                | EVAL ip = TO_STR(host)
                | %s ON ip
                | %s ON ip
                | STATS c = COUNT(*) BY os, location
                | SORT os
                """, test[0], test[1]), null).close());
        }
    }
}
