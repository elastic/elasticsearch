/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

@TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public class CrossClusterLookupJoinIT extends AbstractCrossClusterTestCase {

    public void testLookupJoinAcrossClusters() throws IOException {
        setupClustersAndLookups();

        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        ) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));
            int vIndex = columns.indexOf("v");
            int lookupNameIndex = columns.indexOf("lookup_name");
            int tagIndex = columns.indexOf("tag");
            int lookupTagIndex = columns.indexOf("lookup_tag");

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(20));
            for (var row : values) {
                assertThat(row, hasSize(7));
                Long v = (Long) row.get(vIndex);
                assertThat(v, greaterThanOrEqualTo(0L));
                if (v < 25) {
                    assertThat((String) row.get(lookupNameIndex), equalTo("lookup_" + v));
                    String tag = (String) row.get(tagIndex);
                    if (tag.equals("local")) {
                        assertThat(row.get(lookupTagIndex), equalTo("local"));
                    } else {
                        assertThat(row.get(lookupTagIndex), equalTo(REMOTE_CLUSTER_1));
                    }
                } else {
                    assertNull(row.get(lookupNameIndex));
                    assertNull(row.get(lookupTagIndex));
                }
            }

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    protected Map<String, Object> setupClustersAndLookups() throws IOException {
        var setupData = setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 25);
        return setupData;
    }

    protected void populateLookupIndex(String clusterAlias, String indexName, int numDocs) {
        Client client = client(clusterAlias);
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.mode", "lookup"))
                .setMapping("lookup_key", "type=long", "lookup_name", "type=keyword", "lookup_tag", "type=keyword")
        );
        String tag = Strings.isEmpty(clusterAlias) ? "local" : clusterAlias;
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(indexName).setSource("lookup_key", i, "lookup_name", "lookup_" + i, "lookup_tag", tag).get();
        }
        client.admin().indices().prepareRefresh(indexName).get();
    }

    private static void assertCCSExecutionInfoDetails(EsqlExecutionInfo executionInfo) {
        assertNotNull(executionInfo);
        assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        assertTrue(executionInfo.isCrossClusterSearch());
        List<EsqlExecutionInfo.Cluster> clusters = executionInfo.clusterAliases().stream().map(executionInfo::getCluster).toList();

        for (EsqlExecutionInfo.Cluster cluster : clusters) {
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(0));
        }
    }

}
