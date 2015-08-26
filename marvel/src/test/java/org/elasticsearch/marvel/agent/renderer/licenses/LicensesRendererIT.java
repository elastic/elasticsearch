/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.licenses;

import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.collector.licenses.LicensesCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, transportClientRatio = 0.0)
public class LicensesRendererIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", MarvelPlugin.class.getName() + "," + LicensePlugin.class.getName())
                .put(Node.HTTP_ENABLED, true)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MarvelSettings.STARTUP_DELAY, "1s")
                .put(MarvelSettings.COLLECTORS, LicensesCollector.NAME)
                .build();
    }

    @Test
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/13017")
    public void testLicenses() throws Exception {
        final String clusterUUID = client().admin().cluster().prepareState().setMetaData(true).get().getState().metaData().clusterUUID();
        assertTrue(Strings.hasText(clusterUUID));

        logger.debug("--> waiting for licenses collector to collect data (ie, the trial marvel license)");
        GetResponse response = assertBusy(new Callable<GetResponse>() {
            @Override
            public GetResponse call() throws Exception {
                // Checks if the marvel data index exists (it should have been created by the LicenseCollector)
                assertTrue(MarvelSettings.MARVEL_DATA_INDEX_NAME + " index does not exist", client().admin().indices().prepareExists(MarvelSettings.MARVEL_DATA_INDEX_NAME).get().isExists());
                ensureYellow(MarvelSettings.MARVEL_DATA_INDEX_NAME);

                GetResponse response = client().prepareGet(MarvelSettings.MARVEL_DATA_INDEX_NAME, LicensesCollector.TYPE, clusterUUID).get();
                assertTrue(MarvelSettings.MARVEL_DATA_INDEX_NAME + " document does not exist", response.isExists());
                return response;
            }
        });

        logger.debug("--> checking that the document contains license information");
        assertThat(response.getIndex(), equalTo(MarvelSettings.MARVEL_DATA_INDEX_NAME));
        assertThat(response.getType(), equalTo(LicensesCollector.TYPE));
        assertThat(response.getId(), equalTo(clusterUUID));

        Map<String, Object> source = response.getSource();
        assertThat((String) source.get(LicensesRenderer.Fields.CLUSTER_NAME.underscore().toString()), equalTo(cluster().getClusterName()));
        assertThat((String) source.get(LicensesRenderer.Fields.VERSION.underscore().toString()), equalTo(Version.CURRENT.toString()));

        Object licensesList = source.get(LicensesRenderer.Fields.LICENSES.underscore().toString());
        assertThat(licensesList, instanceOf(List.class));

        List licenses = (List) licensesList;
        assertThat(licenses.size(), equalTo(1));

        Map license = (Map) licenses.iterator().next();
        assertThat(license, instanceOf(Map.class));

        String uid = (String) ((Map) license).get(LicensesRenderer.Fields.UID.underscore().toString());
        assertThat(uid, not(isEmptyOrNullString()));

        String type = (String) ((Map) license).get(LicensesRenderer.Fields.TYPE.underscore().toString());
        assertThat(type, not(isEmptyOrNullString()));

        String status = (String) ((Map) license).get(LicensesRenderer.Fields.STATUS.underscore().toString());
        assertThat(status, not(isEmptyOrNullString()));

        Long expiryDate = (Long) ((Map) license).get(LicensesRenderer.Fields.EXPIRY_DATE_IN_MILLIS.underscore().toString());
        assertThat(expiryDate, greaterThan(0L));

        // We basically recompute the hash here
        String hkey = (String) ((Map) license).get(LicensesRenderer.Fields.HKEY.underscore().toString());
        String recalculated = LicensesRenderer.hash(status, uid, type, String.valueOf(expiryDate), clusterUUID);
        assertThat(hkey, equalTo(recalculated));

        assertThat((String) ((Map) license).get(LicensesRenderer.Fields.FEATURE.underscore().toString()), not(isEmptyOrNullString()));
        assertThat((String) ((Map) license).get(LicensesRenderer.Fields.ISSUER.underscore().toString()), not(isEmptyOrNullString()));
        assertThat((String) ((Map) license).get(LicensesRenderer.Fields.ISSUED_TO.underscore().toString()), not(isEmptyOrNullString()));
        assertThat((Long) ((Map) license).get(LicensesRenderer.Fields.ISSUE_DATE_IN_MILLIS.underscore().toString()), greaterThan(0L));
        assertThat((Integer) ((Map) license).get(LicensesRenderer.Fields.MAX_NODES.underscore().toString()), greaterThan(0));


        logger.debug("--> check that the cluster_licenses is not indexed");
        refresh();
        assertHitCount(client().prepareCount()
                .setIndices(MarvelSettings.MARVEL_DATA_INDEX_NAME)
                .setTypes(LicensesCollector.TYPE)
                .setQuery(QueryBuilders.boolQuery()
                        .should(QueryBuilders.matchQuery(LicensesRenderer.Fields.STATUS.underscore().toString(), "active"))
                        .should(QueryBuilders.matchQuery(LicensesRenderer.Fields.STATUS.underscore().toString(), "inactive"))
                        .should(QueryBuilders.matchQuery(LicensesRenderer.Fields.STATUS.underscore().toString(), "expired"))
                        .minimumNumberShouldMatch(1)
                ).get(), 0L);

    }
}
