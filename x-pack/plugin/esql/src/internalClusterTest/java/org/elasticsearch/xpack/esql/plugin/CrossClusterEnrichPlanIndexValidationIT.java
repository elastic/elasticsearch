/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEnrichBasedCrossClusterTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Plays the part of a querying cluster that rewrites the enrich index in the plan it sends to a remote cluster,
 * and checks that the remote cluster refuses to run the enrich lookup against anything but its own enrich indices.
 */
public class CrossClusterEnrichPlanIndexValidationIT extends AbstractEnrichBasedCrossClusterTestCase {

    private static final String SECRET = "TOP-SECRET";
    private static final String QUERY = "FROM c1:events | EVAL ip = TO_STR(host) | ENRICH _remote:hosts ON ip WITH os | KEEP ip, os";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        return plugins;
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, false);
    }

    @Before
    public void setupRemoteRegularIndex() {
        client(REMOTE_CLUSTER_1).admin()
            .indices()
            .prepareCreate("secrets")
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("ip", "type=keyword", "os", "type=keyword")
            .get();
        client(REMOTE_CLUSTER_1).prepareIndex("secrets").setSource("ip", "192.168.1.2", "os", SECRET).get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh("secrets").get();
    }

    @After
    public void clearSendBehaviors() {
        for (TransportService ts : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
            ((MockTransportService) ts).clearAllRules();
        }
    }

    public void testUnmodifiedRemotePlanEnrichesFromPolicyIndex() {
        try (EsqlQueryResponse resp = runQuery(QUERY, null)) {
            List<Object> osValues = osValues(resp);
            assertThat(osValues, hasItem("Windows"));
            assertThat(osValues, not(hasItem(SECRET)));
        }
    }

    public void testRemotePlanTargetingRegularIndexIsRejected() {
        AtomicInteger rewrittenRequests = RemoteEnrichIndexRewriter.rewriteRemoteEnrichIndex(
            cluster(LOCAL_CLUSTER).getInstances(TransportService.class),
            REMOTE_CLUSTER_1,
            "secrets"
        );
        Exception e = expectThrows(Exception.class, () -> {
            try (EsqlQueryResponse resp = runQuery(QUERY, null)) {
                fail("ENRICH on the remote cluster read [secrets] and returned " + osValues(resp));
            }
        });
        assertThat(rewrittenRequests.get(), greaterThan(0));
        assertThat(e.getMessage(), containsString("enrich lookup on index [secrets] is not allowed"));
    }

    private static List<Object> osValues(EsqlQueryResponse resp) {
        return getValuesList(resp).stream().map(row -> row.get(1)).toList();
    }
}
