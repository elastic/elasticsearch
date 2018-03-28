/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

public class IndexAuditUpgradeIT extends AbstractUpgradeTestCase {

    private Version minVersionInCluster;

    @Before
    public void findMinVersionInCluster() throws IOException {
        Response response = client().performRequest("GET", "_nodes");
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Version version = Version.CURRENT;
        for (String id : nodesAsMap.keySet()) {
            Version nodeVersion = Version.fromString(objectPath.evaluate("nodes." + id + ".version"));
            if (nodeVersion.before(version)) {
                version = nodeVersion;
            }
        }
        minVersionInCluster = version;
    }

    public void testDocsAuditedInOldCluster() throws Exception {
        assumeTrue("only runs against old cluster", clusterType == CLUSTER_TYPE.OLD);
        final Matcher<Integer> expectedBuckets = minVersionInCluster.onOrAfter(Version.V_6_2_3) ? is(2) : is(0);
        assertBusy(() -> {
            assertAuditDocsExist();
            assertNumUniqueNodeNameBuckets(expectedBuckets);
        });
    }

    public void testDocsAuditedInMixedCluster() throws Exception {
        assumeTrue("only runs against mixed cluster", clusterType == CLUSTER_TYPE.MIXED);
        // the isOneOf(0, 1) check is necessary for instances where this test runs across an
        // an index rollover and the audit trail on the upgraded node starts so we get a bucket
        // with a node name
        final Matcher<Integer> expectedBuckets = minVersionInCluster.onOrAfter(Version.V_6_2_3) ? is(2) : isOneOf(0, 1);
        assertBusy(() -> {
            assertAuditDocsExist();
            assertNumUniqueNodeNameBuckets(expectedBuckets);
        });
    }

    public void testDocsAuditedInUpgradedCluster() throws Exception {
        assumeTrue("only runs against upgraded cluster", clusterType == CLUSTER_TYPE.UPGRADED);
        // cannot use minVersion in cluster as we are upgraded on all nodes,
        // BUT we can check the version created on the index
        Response response = client().performRequest("GET", "/.security_audit_log*/_settings/index.version.created",
                Collections.singletonMap("flat_settings", "true"));
        Map<String, Object> responseMap = entityAsMap(response);
        logger.error("get settings response {}", responseMap);
        Version minVersion = null;
        for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
            Map<String, Object> indexEntry = (Map<String, Object>) entry.getValue();
            Map<String, Object> indexSettings = (Map<String, Object>) indexEntry.get("settings");
            String versionCreated = (String) indexSettings.get("index.version.created");
            if (versionCreated != null) {
                Version indexVersionCreated = Version.fromId(Integer.valueOf(versionCreated));
                if (minVersion == null || indexVersionCreated.before(minVersion)) {
                    minVersion = indexVersionCreated;
                }
            }
        }

        assertNotNull(minVersion);
        final Matcher<Integer> expectedBuckets = minVersion.onOrAfter(Version.V_6_2_3) ? is(4) : is(2);
        assertBusy(() -> {
            assertAuditDocsExist();
            assertNumUniqueNodeNameBuckets(expectedBuckets);
        });
    }

    private void assertAuditDocsExist() throws Exception {
        final String type = minVersionInCluster.before(Version.V_6_0_0) ? "event" : "doc";
        Response response = client().performRequest("GET", "/.security_audit_log*/" + type + "/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull(responseMap.get("count"));
        assertThat((Integer) responseMap.get("count"), Matchers.greaterThanOrEqualTo(1));
    }

    private void assertNumUniqueNodeNameBuckets(Matcher<Integer> numBucketsMatcher) throws Exception {
        // call API that will hit all nodes
        assertEquals(200, client().performRequest("GET", "/_nodes").getStatusLine().getStatusCode());

        HttpEntity httpEntity = new StringEntity(
                "{\n" +
                        "    \"aggs\" : {\n" +
                        "        \"nodes\" : {\n" +
                        "            \"terms\" : { \"field\" : \"node_name\" }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}", ContentType.APPLICATION_JSON);
        Response aggResponse = client().performRequest("GET", "/.security_audit_log*/_search",
                Collections.singletonMap("pretty", "true"), httpEntity);
        Map<String, Object> aggResponseMap = entityAsMap(aggResponse);
        logger.debug("aggResponse {}", aggResponseMap);
        Map<String, Object> aggregations = (Map<String, Object>) aggResponseMap.get("aggregations");
        assertNotNull(aggregations);
        Map<String, Object> nodesAgg = (Map<String, Object>) aggregations.get("nodes");
        assertNotNull(nodesAgg);
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) nodesAgg.get("buckets");
        assertNotNull(buckets);
        assertThat(buckets.size(), numBucketsMatcher);
    }
}
