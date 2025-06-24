/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.esql.VerificationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class CrossClusterLookupJoinUnsupportedIT extends AbstractCrossClusterTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(CrossClusterEnrichIT.LocalStateEnrich.class);
        plugins.remove(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(IngestCommonPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    public void testLookupJoinUnsupported() throws IOException {
        setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 25);

        var ex = expectThrows(
            VerificationException.class,
            () -> runQuery(
                "FROM logs-*,c*:logs-* | STATS c = COUNT(*) by v | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        );
        assertThat(ex.getMessage(), containsString("LOOKUP JOIN with remote indices can't be executed after STATS"));

        ex = expectThrows(
            VerificationException.class,
            () -> runQuery(
                "FROM logs-*,c*:logs-* | SORT v | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        );
        assertThat(ex.getMessage(), containsString("LOOKUP JOIN with remote indices can't be executed after SORT"));

        setupHostsEnrich();
        ex = expectThrows(
            VerificationException.class,
            () -> runQuery(
                "FROM logs-*,c*:logs-* | EVAL ip = id | ENRICH _coordinator:hosts | EVAL lookup_key = v "
                    + "| LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        );
        assertThat(
            ex.getMessage(),
            containsString("LOOKUP JOIN with remote indices can't be executed after ENRICH with coordinator policy")
        );
    }

    public void setupHostsEnrich() {
        // the hosts policy are identical on every node
        Map<String, String> allHosts = Map.of("192.168.1.2", "Windows");
        Client client = client(LOCAL_CLUSTER);
        client.admin().indices().prepareCreate("hosts").setMapping("ip", "type=ip", "os", "type=keyword").get();
        for (Map.Entry<String, String> h : allHosts.entrySet()) {
            client.prepareIndex("hosts").setSource("ip", h.getKey(), "os", h.getValue()).get();
        }
        client.admin().indices().prepareRefresh("hosts").get();
        EnrichPolicy hostPolicy = new EnrichPolicy("match", null, List.of("hosts"), "ip", List.of("ip", "os"));
        client.execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "hosts", hostPolicy))
            .actionGet();
        client.execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "hosts"))
            .actionGet();
        assertAcked(client.admin().indices().prepareDelete("hosts"));
    }
}
