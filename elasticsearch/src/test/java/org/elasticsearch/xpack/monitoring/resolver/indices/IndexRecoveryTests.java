/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryCollector;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = TEST)
public class IndexRecoveryTests extends MonitoringIntegTestCase {

    private static final String INDEX_PREFIX = "test-index-recovery-";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put(MonitoringSettings.INDICES.getKey(), INDEX_PREFIX + "*")
                .put(MonitoringSettings.COLLECTORS.getKey(), IndexRecoveryCollector.NAME)
                .put("xpack.monitoring.exporters.default_local.type", "local")
                .build();
    }

    @After
    public void cleanup() throws Exception {
        disableMonitoringInterval();
        wipeMonitoringIndices();
    }

    public void testIndexRecovery() throws Exception {
        logger.debug("--> creating some indices so that index recovery collector reports data");
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            client().prepareIndex(INDEX_PREFIX + i, "foo").setSource("field1", "value1").get();
        }

        logger.debug("--> wait for index recovery collector to collect data");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                flush();
                refresh();

                RecoveryResponse recoveries = client().admin().indices().prepareRecoveries().get();
                assertThat(recoveries.hasRecoveries(), is(true));
            }
        });

        updateMonitoringInterval(3L, TimeUnit.SECONDS);
        waitForMonitoringIndices();

        awaitMonitoringDocsCount(greaterThan(0L), IndexRecoveryResolver.TYPE);

        String clusterUUID = client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID();
        assertTrue(Strings.hasText(clusterUUID));

        logger.debug("--> searching for monitoring documents of type [{}]", IndexRecoveryResolver.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(IndexRecoveryResolver.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = {
                MonitoringIndexNameResolver.Fields.CLUSTER_UUID,
                MonitoringIndexNameResolver.Fields.TIMESTAMP,
                MonitoringIndexNameResolver.Fields.SOURCE_NODE,
                IndexRecoveryResolver.Fields.INDEX_RECOVERY,
                IndexRecoveryResolver.Fields.INDEX_RECOVERY + "."
                        + IndexRecoveryResolver.Fields.SHARDS,
        };

        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.getSourceAsMap();
            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        flush();
        refresh();

        logger.debug("--> checking that cluster_uuid field is correctly indexed");
        response = client().prepareSearch().setTypes(IndexRecoveryResolver.TYPE).setSize(0).setQuery(existsQuery("cluster_uuid")).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that timestamp field is correctly indexed");
        response = client().prepareSearch().setTypes(IndexRecoveryResolver.TYPE).setSize(0).setQuery(existsQuery("timestamp")).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that other fields are not indexed");
        String[] fields = {
                "index_recovery.shards.primary",
                "index_recovery.shards.id",
                "index_recovery.shards.stage",
                "index_recovery.shards.index_name",
                "index_recovery.shards.source.host",
                "index_recovery.shards.source.name",
        };

        for (String field : fields) {
            response = client().prepareSearch().setTypes(IndexRecoveryResolver.TYPE).setSize(0).setQuery(existsQuery(field)).get();
            assertHitCount(response, 0L);
        }

        logger.debug("--> index recovery successfully collected");
    }
}
