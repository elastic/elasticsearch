/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.License;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = TEST)
public class ClusterInfoTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put(MonitoringSettings.COLLECTORS.getKey(), ClusterStatsCollector.NAME)
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

    public void testClusterInfo() throws Exception {
        ensureGreen();

        final String clusterUUID = client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID();
        assertTrue(Strings.hasText(clusterUUID));

        // waiting for the monitoring data index to be created (it should have been created by the ClusterInfoCollector
        String dataIndex = ".monitoring-data-" + MonitoringTemplateUtils.TEMPLATE_VERSION;
        awaitIndexExists(dataIndex);

        // waiting for cluster info collector to collect data
        awaitMonitoringDocsCount(equalTo(1L), ClusterInfoResolver.TYPE);

        // retrieving cluster info document
        GetResponse response = client().prepareGet(dataIndex, ClusterInfoResolver.TYPE, clusterUUID).get();
        assertTrue("cluster_info document does not exist in data index", response.isExists());

        assertThat(response.getIndex(), equalTo(dataIndex));
        assertThat(response.getType(), equalTo(ClusterInfoResolver.TYPE));
        assertThat(response.getId(), equalTo(clusterUUID));

        Map<String, Object> source = response.getSource();
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
        String recalculated = ClusterInfoResolver.hash(status, uid, type, String.valueOf(expiryDate), clusterUUID);
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

        waitForMonitoringTemplates();

        // check that the cluster_info is not indexed
        flush();
        refresh();

        assertHitCount(client().prepareSearch().setSize(0)
                .setIndices(dataIndex)
                .setTypes(ClusterInfoResolver.TYPE)
                .setQuery(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.matchQuery(License.Fields.STATUS, License.Status.ACTIVE.label()))
                        .should(QueryBuilders.matchQuery(License.Fields.STATUS, License.Status.INVALID.label()))
                        .should(QueryBuilders.matchQuery(License.Fields.STATUS, License.Status.EXPIRED.label()))
                        .should(QueryBuilders.matchQuery("cluster_name", cluster().getClusterName()))
                        .minimumShouldMatch(1)
                ).get(), 0L);
    }
}
