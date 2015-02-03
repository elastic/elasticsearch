/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 0)
public class NoMasterNodeTests extends AbstractAlertingTests {

    private ClusterDiscoveryConfiguration.UnicastZen config;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings unicastSettings = config.node(nodeOrdinal);
        return ImmutableSettings.builder()
                .put(settings)
                .put(unicastSettings)
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 2)
                .put("discovery.type", "zen")
                .build();
    }

    @Test
    public void testSimpleFailure() throws Exception {
        config = new ClusterDiscoveryConfiguration.UnicastZen(2);
        internalTestCluster().startNodesAsync(2).get();
        createIndex("my-index");
        ensureAlertingStarted();

        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        alertClient().preparePutAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();
        assertAlertTriggered("my-first-alert", 1);

        // Stop the elected master, no new master will be elected b/c of m_m_n is set to 2
        stopElectedMasterNodeAndWait();
        try {
            // any alerting action should fail, because there is no elected master node
            alertClient().prepareDeleteAlert("my-first-alert").setMasterNodeTimeout(TimeValue.timeValueSeconds(1)).get();
            fail();
        } catch (Exception e) {
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(MasterNotDiscoveredException.class));
        }
        // Bring back the 2nd node and wait for elected master node to come back and alerting to work as expected.
        startElectedMasterNodeAndWait();

        // Our first alert should at least have been triggered twice
        assertAlertTriggered("my-first-alert", 2);

        // Delete the existing alert
        DeleteAlertResponse response = alertClient().prepareDeleteAlert("my-first-alert").get();
        assertThat(response.deleteResponse().isFound(), is(true));

        // Add a new alert and wait for it get triggered
        alertClient().preparePutAlert("my-second-alert")
                .setAlertSource(alertSource)
                .get();
        assertAlertTriggered("my-second-alert", 1);
    }

    @Test
    public void testMultipleFailures() throws Exception {
        int numberOfFailures = scaledRandomIntBetween(2, 9);
        int numberOfAlerts = scaledRandomIntBetween(numberOfFailures, 12);
        logger.info("Number of failures [{}], number of alerts [{}]", numberOfFailures, numberOfAlerts);
        config = new ClusterDiscoveryConfiguration.UnicastZen(2 + numberOfFailures);
        internalTestCluster().startNodesAsync(2).get();
        createIndex("my-index");
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();

        // Alerting starts in the background, it can happen we get here too soon, so wait until alerting has started.
        ensureAlertingStarted();
        for (int i = 1; i <= numberOfAlerts; i++) {
            String alertName = "alert" + i;
            SearchRequest searchRequest = createTriggerSearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
            BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
            alertClient().preparePutAlert(alertName)
                    .setAlertSource(alertSource)
                    .get();
        }

        for (int i = 1; i <= numberOfFailures; i++) {
            logger.info("Failure round {}", i);

            for (int j = 1; j < numberOfAlerts; j++) {
                String alertName = "alert" + i;
                assertAlertTriggered(alertName, i);
            }
            stopElectedMasterNodeAndWait();
            startElectedMasterNodeAndWait();

            AlertsStatsResponse statsResponse = alertClient().prepareAlertsStats().get();
            assertThat(statsResponse.getNumberOfRegisteredAlerts(), equalTo((long) numberOfAlerts));
        }
    }

    private void stopElectedMasterNodeAndWait() throws Exception {
        internalTestCluster().stopCurrentMasterNode();
        // Can't use ensureAlertingStopped, b/c that relies on the alerts stats api which requires an elected master node
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                for (Client client : clients()) {
                    ClusterState state = client.admin().cluster().prepareState().setLocal(true).get().getState();
                    if (!state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID)) {
                        return false;
                    }
                }
                return true;
            }
        }), equalTo(true));
        // Ensure that the alert manager doesn't run elsewhere
        for (AlertsService alertsService : internalTestCluster().getInstances(AlertsService.class)) {
            assertThat(alertsService.getState(), is(AlertsService.State.STOPPED));
        }
    }

    private void startElectedMasterNodeAndWait() throws Exception {
        internalTestCluster().startNode();
        ensureAlertingStarted();
    }

}
