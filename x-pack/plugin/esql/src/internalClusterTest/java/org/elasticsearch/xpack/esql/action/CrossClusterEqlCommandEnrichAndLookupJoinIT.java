/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterEqlCommandEnrichAndLookupJoinIT extends AbstractEnrichBasedCrossClusterTestCase {

    private static final String LOCAL_EQL_SOURCE = "EQL eql_events \"process where true\"";
    private static final String REMOTE_EQL_SOURCE = "EQL " + REMOTE_CLUSTER_1 + ":eql_events \"process where true\"";
    private static final String EQL_REMOTE_ENRICH_REASON =
        "the EQL source command returns its events to the coordinating node and is incompatible with remote ENRICH";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EqlPlugin.class);
        return plugins;
    }

    @Before
    public void setupEqlEventsAndLookup() {
        assumeTrue("EQL command is snapshot-only", Build.current().isSnapshot());

        createEqlEvents(client(LOCAL_CLUSTER));
        createEqlEvents(client(REMOTE_CLUSTER_1));

        Client local = client(LOCAL_CLUSTER);
        assertAcked(
            local.admin()
                .indices()
                .prepareCreate("os_lookup")
                .setSettings(Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP))
                .setMapping("os", "type=keyword", "family", "type=keyword")
        );
        local.prepareBulk()
            .add(new IndexRequest("os_lookup").source("os", "Android", "family", "mobile"))
            .add(new IndexRequest("os_lookup").source("os", "Linux", "family", "desktop"))
            .add(new IndexRequest("os_lookup").source("os", "MacOS", "family", "desktop"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private static void createEqlEvents(Client client) {
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate("eql_events")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("@timestamp", "type=date", "event.category", "type=keyword", "os", "type=keyword")
        );
        client.prepareBulk()
            .add(new IndexRequest("eql_events").source("@timestamp", "2026-07-22T10:00:00Z", "event.category", "process", "os", "Android"))
            .add(new IndexRequest("eql_events").source("@timestamp", "2026-07-22T10:00:01Z", "event.category", "network", "os", "MacOS"))
            .add(new IndexRequest("eql_events").source("@timestamp", "2026-07-22T10:00:02Z", "event.category", "process", "os", "Linux"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testLookupJoinAfterEqlUsesLocalLookupIndex() {
        for (String eqlSource : List.of(LOCAL_EQL_SOURCE, REMOTE_EQL_SOURCE)) {
            for (String lookupIndex : List.of("os_lookup", "_coordinator:os_lookup")) {
                String query = eqlSource + " | LOOKUP JOIN " + lookupIndex + " ON os | KEEP os, family | SORT os";
                try (EsqlQueryResponse resp = runQuery(query, null)) {
                    assertThat(query, getValuesList(resp), equalTo(List.of(List.of("Android", "mobile"), List.of("Linux", "desktop"))));
                }
            }
        }
    }

    public void testEnrichAfterEqlUsesLocalPolicy() {
        for (String eqlSource : List.of(LOCAL_EQL_SOURCE, REMOTE_EQL_SOURCE)) {
            for (Enrich.Mode mode : List.of(Enrich.Mode.ANY, Enrich.Mode.COORDINATOR)) {
                String query = eqlSource + " | " + enrichVendors(mode) + " | KEEP os, vendor | SORT os";
                try (EsqlQueryResponse resp = runQuery(query, null)) {
                    assertThat(query, getValuesList(resp), equalTo(List.of(List.of("Android", "Samsung"), List.of("Linux", "Redhat"))));
                }
            }
        }
    }

    public void testRemoteEnrichAfterEqlIsRejected() {
        for (String eqlSource : List.of(LOCAL_EQL_SOURCE, REMOTE_EQL_SOURCE)) {
            for (String enrich : List.of(
                "ENRICH _remote:vendors ON os",
                "EVAL ip = \"192.168.1.3\" | ENRICH _remote:hosts_local ON ip WITH host_os = os"
            )) {
                String query = eqlSource + " | " + enrich;
                expectThrows(
                    VerificationException.class,
                    allOf(
                        containsString("ENRICH with remote policy can't be executed after [" + eqlSource + "]"),
                        containsString(EQL_REMOTE_ENRICH_REASON)
                    ),
                    () -> runQuery(query, randomBoolean()).close()
                );
            }
        }
    }

    public void testRemoteEnrichAfterRemoteEqlWithPolicyOnlyOnRemoteIsRejected() {
        Client remote = client(REMOTE_CLUSTER_1);
        remote.admin().indices().prepareCreate("os_remote_only").setMapping("os", "type=keyword", "vendor", "type=keyword").get();
        remote.prepareIndex("os_remote_only").setSource("os", "Android", "vendor", "Google").get();
        remote.admin().indices().prepareRefresh("os_remote_only").get();
        String policyName = "os_remote_only";
        EnrichPolicy policy = new EnrichPolicy("match", null, List.of(policyName), "os", List.of("os", "vendor"));
        remote.execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName, policy))
            .actionGet();
        try {
            remote.execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName))
                .actionGet();
            String query = REMOTE_EQL_SOURCE + " | ENRICH _remote:" + policyName + " ON os";
            expectThrows(
                VerificationException.class,
                allOf(
                    containsString("ENRICH with remote policy can't be executed after [" + REMOTE_EQL_SOURCE + "]"),
                    containsString(EQL_REMOTE_ENRICH_REASON)
                ),
                () -> runQuery(query, randomBoolean()).close()
            );
        } finally {
            remote.execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName))
                .actionGet();
        }
    }

    public void testLookupJoinThenRemoteEnrichAfterRemoteEqlIsRejected() {
        String query = REMOTE_EQL_SOURCE + " | LOOKUP JOIN os_lookup ON os | ENRICH _remote:vendors ON os";
        expectThrows(
            VerificationException.class,
            containsString("can't be executed after [" + REMOTE_EQL_SOURCE + "]"),
            () -> runQuery(query, randomBoolean()).close()
        );
    }
}
