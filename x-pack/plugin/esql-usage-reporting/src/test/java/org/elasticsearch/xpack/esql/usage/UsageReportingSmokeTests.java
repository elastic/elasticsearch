/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.esql.usage.testfixture.FakeUsageApi;
import org.elasticsearch.xpack.esql.usage.testfixture.FakeUsageApi.Behavior;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * A node with the plugin enabled delivers usage to a Usage API over mutual TLS: the settings, the SSL profile of x-pack-core, the
 * listener, the reporter and the publisher are the real ones, and only the Usage API is the {@link FakeUsageApi}. It is a smoke test
 * of the wiring, which the unit tests of each piece cannot show; the behavior of each piece is tested next to it.
 * <p>
 * The queries are not run: the listener is called as the ES|QL plugin calls it when an external-source query completes, because
 * the ES|QL integration test infrastructure is not available to this plugin. For the same reason the listener is built by hand: the
 * service registration through which ES|QL creates it is not exercised here.
 */
public class UsageReportingSmokeTests extends ESSingleNodeTestCase {

    private static FakeUsageApi api;

    @BeforeClass
    public static void startApi() throws Exception {
        api = FakeUsageApi.start();
    }

    @AfterClass
    public static void stopApi() {
        api.close();
    }

    @Before
    public void resetApi() {
        api.reset();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateEsqlUsageReporting.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(api.pluginSettings().build())
            .put(UsageReportingSettings.ENABLED.getKey(), true)
            .put(UsageReportingSettings.PERIOD.getKey(), "1s")
            .build();
    }

    private UsageListener listener() {
        LocalStateEsqlUsageReporting local = getInstanceFromNode(PluginsService.class).filterPlugins(LocalStateEsqlUsageReporting.class)
            .findFirst()
            .orElseThrow();
        return new UsageListener(local.usagePlugin());
    }

    /** The sum of the quantity (milliseconds) of the records of every request received, whatever the fixture answered. */
    private long receivedQuantity() {
        long total = 0;
        for (FakeUsageApi.ReceivedRequest request : api.requests()) {
            for (Map<String, Object> record : request.records()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> usage = (Map<String, Object>) record.get("usage");
                total += ((Number) usage.get("quantity")).longValue();
            }
        }
        return total;
    }

    public void testACompletedQueryIsDeliveredWithTheIdentityOfTheNode() throws Exception {
        // 3 ms of execution and 2 ms of reading
        listener().onQueryCompleted(Map.of(QueryMetricsListener.CPU_NANOS, 3_000_000L, QueryMetricsListener.READ_CPU_NANOS, 2_000_000L));

        assertBusy(() -> assertThat(api.acceptedRecordIds(), not(empty())));

        FakeUsageApi.ReceivedRequest request = api.firstRequest(r -> r.recordIds().isEmpty() == false);
        assertThat(request.clientSubject(), equalTo(FakeUsageApi.CLIENT_SUBJECT));
        Map<String, Object> record = request.records().get(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> usage = (Map<String, Object>) record.get("usage");
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) record.get("source");
        @SuppressWarnings("unchecked")
        Map<String, Object> sourceMetadata = (Map<String, Object>) source.get("metadata");
        assertThat(usage.get("type"), equalTo(UsageTypes.EXTERNAL_SOURCES_CPU));
        assertThat(((Number) usage.get("quantity")).longValue(), equalTo(5L));
        // the first window after the start of the node is shorter than the period, and the seconds are truncated
        assertThat(((Number) usage.get("period_seconds")).longValue(), greaterThanOrEqualTo(0L));
        assertThat(source.get("instance_group_id"), equalTo("fake-deployment"));
        assertThat(source.get("provider"), equalTo("aws"));
        assertThat(source.get("region"), equalTo("eu-west-1"));
        assertThat(sourceMetadata.get("cluster_id"), equalTo("fake-cluster"));
        assertThat(receivedQuantity(), equalTo(5L));
    }

    public void testARecordThatTheUsageApiAnswersWith503IsSentAgainWithTheSameId() throws Exception {
        api.enqueue(Behavior.always(503));
        listener().onQueryCompleted(Map.of(QueryMetricsListener.CPU_NANOS, 7_000_000L));

        assertBusy(() -> assertThat(api.acceptedRecordIds(), not(empty())));

        assertThat("the id is the same on the retry", api.receivedRecordIds().size(), equalTo(2));
        assertThat(api.receivedRecordIds().get(0), equalTo(api.receivedRecordIds().get(1)));
        assertThat(api.acceptedRecordIds().size(), equalTo(1));
    }
}
