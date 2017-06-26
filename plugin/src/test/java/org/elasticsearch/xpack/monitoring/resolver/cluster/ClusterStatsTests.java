/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.License;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = TEST)
public class ClusterStatsTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .build();
    }

    @Before
    public void init() throws Exception {
        updateMonitoringInterval(3L, TimeUnit.SECONDS);
    }

    @After
    public void cleanup() throws Exception {
        disableMonitoringInterval();
        wipeMonitoringIndices();
    }

    @SuppressWarnings("unchecked")
    public void testClusterStats() throws Exception {
        ensureGreen();

        final String clusterUUID = client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID();
        assertTrue(Strings.hasText(clusterUUID));

        // waiting for cluster stats collector to collect data
        awaitMonitoringDocsCountOnPrimary(greaterThanOrEqualTo(1L), ClusterStatsMonitoringDoc.TYPE);

        refresh();

        // retrieving cluster stats document
        final SearchResponse response =
                client().prepareSearch(".monitoring-es-*")
                        .setQuery(QueryBuilders.termQuery("type", ClusterStatsMonitoringDoc.TYPE))
                        .addSort("timestamp", SortOrder.DESC)
                        .setSize(1)
                        .setPreference("_primary").get();
        assertTrue("cluster_stats document does not exist", response.getHits().getTotalHits() != 0);

        final SearchHit hit = response.getHits().getAt(0);

        assertTrue("_source is disabled", hit.hasSource());

        Map<String, Object> source = hit.getSourceAsMap();

        assertThat(source.get(MonitoringIndexNameResolver.Fields.CLUSTER_UUID), notNullValue());
        assertThat(source.get(MonitoringIndexNameResolver.Fields.TIMESTAMP), notNullValue());
        assertThat(source.get(MonitoringIndexNameResolver.Fields.SOURCE_NODE), notNullValue());
        assertThat(source.get("cluster_name"), equalTo(cluster().getClusterName()));
        assertThat(source.get("version"), equalTo(Version.CURRENT.toString()));

        Object licenseObj = source.get("license");
        assertThat(licenseObj, instanceOf(Map.class));
        Map license = (Map) licenseObj;

        assertThat(license, instanceOf(Map.class));

        String uid = (String) license.get("uid");
        assertThat(uid, not(isEmptyOrNullString()));

        String type = (String) license.get("type");
        assertThat(type, not(isEmptyOrNullString()));

        String status = (String) license.get(License.Fields.STATUS);
        assertThat(status, not(isEmptyOrNullString()));

        Long expiryDate = (Long) license.get(License.Fields.EXPIRY_DATE_IN_MILLIS);
        assertThat(expiryDate, greaterThan(0L));

        // We basically recompute the hash here
        String hkey = (String) license.get("hkey");
        String recalculated = ClusterStatsResolver.hash(status, uid, type, String.valueOf(expiryDate), clusterUUID);
        assertThat(hkey, equalTo(recalculated));

        assertThat((String) license.get(License.Fields.ISSUER), not(isEmptyOrNullString()));
        assertThat((String) license.get(License.Fields.ISSUED_TO), not(isEmptyOrNullString()));
        assertThat((Long) license.get(License.Fields.ISSUE_DATE_IN_MILLIS), greaterThan(0L));
        assertThat((Integer) license.get(License.Fields.MAX_NODES), greaterThan(0));

        Object clusterStats = source.get("cluster_stats");
        assertNotNull(clusterStats);
        assertThat(clusterStats, instanceOf(Map.class));
        assertThat(((Map) clusterStats).size(), greaterThan(0));

        Object stackStats = source.get("stack_stats");
        assertNotNull(stackStats);
        assertThat(stackStats, instanceOf(Map.class));
        assertThat(((Map) stackStats).size(), equalTo(1));

        Object xpack = ((Map)stackStats).get("xpack");
        assertNotNull(xpack);
        assertThat(xpack, instanceOf(Map.class));
        // it must have at least monitoring, but others may be hidden
        assertThat(((Map) xpack).size(), greaterThanOrEqualTo(1));

        Object monitoring = ((Map)xpack).get("monitoring");
        assertNotNull(monitoring);
        // we don't make any assumptions about what's in it, only that it's there
        assertThat(monitoring, instanceOf(Map.class));

        Object clusterState = source.get("cluster_state");
        assertNotNull(clusterState);
        assertThat(clusterState, instanceOf(Map.class));

        Map<String, Object> clusterStateMap = (Map<String, Object>)clusterState;

        assertThat(clusterStateMap.keySet(), hasSize(5));
        assertThat(clusterStateMap.remove("status"), notNullValue());
        assertThat(clusterStateMap.remove("version"), notNullValue());
        assertThat(clusterStateMap.remove("state_uuid"), notNullValue());
        assertThat(clusterStateMap.remove("master_node"), notNullValue());
        assertThat(clusterStateMap.remove("nodes"), notNullValue());
    }
}
