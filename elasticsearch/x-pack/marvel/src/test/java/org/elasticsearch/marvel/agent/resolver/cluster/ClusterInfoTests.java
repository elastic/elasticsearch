/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.core.License;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = TEST)
public class ClusterInfoTests extends MarvelIntegTestCase {

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
        updateMarvelInterval(3L, TimeUnit.SECONDS);
    }

    @After
    public void cleanup() throws Exception {
        updateMarvelInterval(-1, TimeUnit.SECONDS);
        wipeMarvelIndices();
    }

    public void testClusterInfo() throws Exception {
        securedEnsureGreen();

        final String clusterUUID = client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID();
        assertTrue(Strings.hasText(clusterUUID));

        logger.debug("--> waiting for the monitoring data index to be created (it should have been created by the ClusterInfoCollector)");
        String dataIndex = ".monitoring-data-" + MarvelTemplateUtils.TEMPLATE_VERSION;
        awaitIndexExists(dataIndex);

        logger.debug("--> waiting for cluster info collector to collect data");
        awaitMarvelDocsCount(equalTo(1L), ClusterInfoResolver.TYPE);

        logger.debug("--> retrieving cluster info document");
        GetResponse response = client().prepareGet(dataIndex, ClusterInfoResolver.TYPE, clusterUUID).get();
        assertTrue("cluster_info document does not exist in data index", response.isExists());

        assertThat(response.getIndex(), equalTo(dataIndex));
        assertThat(response.getType(), equalTo(ClusterInfoResolver.TYPE));
        assertThat(response.getId(), equalTo(clusterUUID));

        Map<String, Object> source = response.getSource();
        assertThat(source.get(MonitoringIndexNameResolver.Fields.CLUSTER_UUID), notNullValue());
        assertThat(source.get(MonitoringIndexNameResolver.Fields.TIMESTAMP), notNullValue());
        assertThat(source.get(MonitoringIndexNameResolver.Fields.SOURCE_NODE), notNullValue());
        assertThat(source.get(ClusterInfoResolver.Fields.CLUSTER_NAME), equalTo(cluster().getClusterName()));
        assertThat(source.get(ClusterInfoResolver.Fields.VERSION), equalTo(Version.CURRENT.toString()));

        logger.debug("--> checking that the document contains license information");
        Object licenseObj = source.get(ClusterInfoResolver.Fields.LICENSE);
        assertThat(licenseObj, instanceOf(Map.class));
        Map license = (Map) licenseObj;

        assertThat(license, instanceOf(Map.class));

        String uid = (String) license.get(ClusterInfoResolver.Fields.UID);
        assertThat(uid, not(isEmptyOrNullString()));

        String type = (String) license.get(ClusterInfoResolver.Fields.TYPE);
        assertThat(type, not(isEmptyOrNullString()));

        String status = (String) license.get(License.Fields.STATUS);
        assertThat(status, not(isEmptyOrNullString()));

        Long expiryDate = (Long) license.get(License.Fields.EXPIRY_DATE_IN_MILLIS);
        assertThat(expiryDate, greaterThan(0L));

        // We basically recompute the hash here
        String hkey = (String) license.get(ClusterInfoResolver.Fields.HKEY);
        String recalculated = ClusterInfoResolver.hash(status, uid, type, String.valueOf(expiryDate), clusterUUID);
        assertThat(hkey, equalTo(recalculated));

        assertThat((String) license.get(License.Fields.ISSUER), not(isEmptyOrNullString()));
        assertThat((String) license.get(License.Fields.ISSUED_TO), not(isEmptyOrNullString()));
        assertThat((Long) license.get(License.Fields.ISSUE_DATE_IN_MILLIS), greaterThan(0L));
        assertThat((Integer) license.get(License.Fields.MAX_NODES), greaterThan(0));

        Object clusterStats = source.get(ClusterInfoResolver.Fields.CLUSTER_STATS);
        assertNotNull(clusterStats);
        assertThat(clusterStats, instanceOf(Map.class));
        assertThat(((Map) clusterStats).size(), greaterThan(0));

        waitForMarvelTemplates();

        logger.debug("--> check that the cluster_info is not indexed");
        securedFlush();
        securedRefresh();

        assertHitCount(client().prepareSearch().setSize(0)
                .setIndices(dataIndex)
                .setTypes(ClusterInfoResolver.TYPE)
                .setQuery(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.matchQuery(License.Fields.STATUS, License.Status.ACTIVE.label()))
                        .should(QueryBuilders.matchQuery(License.Fields.STATUS, License.Status.INVALID.label()))
                        .should(QueryBuilders.matchQuery(License.Fields.STATUS, License.Status.EXPIRED.label()))
                        .should(QueryBuilders.matchQuery(ClusterInfoResolver.Fields.CLUSTER_NAME,
                                cluster().getClusterName()))
                        .minimumNumberShouldMatch(1)
                ).get(), 0L);
    }
}
