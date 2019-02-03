/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.common.Booleans;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasSize;

public class IndexAuditUpgradeIT extends AbstractUpgradeTestCase {
    private Version minVersionInCluster;

    @Before
    public void findMinVersionInCluster() throws IOException {
        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            // cannot use minVersion in cluster as we are upgraded on all nodes,
            // BUT we can check the version created on the index
            Response response = client().performRequest("GET", "/.security_audit_log*/_settings/index.version.created",
                    Collections.singletonMap("flat_settings", "true"));
            Map<String, Object> responseMap = entityAsMap(response);
            logger.info("get settings response {}", responseMap);
            for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                Map<String, Object> indexEntry = (Map<String, Object>) entry.getValue();
                Map<String, Object> indexSettings = (Map<String, Object>) indexEntry.get("settings");
                String versionCreated = (String) indexSettings.get("index.version.created");
                if (versionCreated != null) {
                    Version indexVersionCreated = Version.fromId(Integer.valueOf(versionCreated));
                    if (minVersionInCluster == null || indexVersionCreated.before(minVersionInCluster)) {
                        minVersionInCluster = indexVersionCreated;
                    }
                }
            }

            assertNotNull(minVersionInCluster);
        } else {
            Response response = client().performRequest("GET", "_nodes");
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
            minVersionInCluster = Version.CURRENT;
            for (String id : nodesAsMap.keySet()) {
                Version nodeVersion = Version.fromString(objectPath.evaluate("nodes." + id + ".version"));
                if (nodeVersion.before(minVersionInCluster)) {
                    minVersionInCluster = nodeVersion;
                }
            }

            assertTrue(minVersionInCluster + " is older than " + Version.CURRENT,
                    minVersionInCluster.before(Version.CURRENT));
        }
    }

    public void testAuditLogs() throws Exception {
        assertBusy(() -> {
            assertAuditDocsExist();
            assertNumUniqueNodeNameBuckets(expectedNumUniqueNodeNameBuckets());
        }, 30, TimeUnit.SECONDS);
    }

    private int expectedNumUniqueNodeNameBuckets() throws IOException {
        int oldVal = minVersionInCluster.onOrAfter(Version.V_6_2_3) ? 3 : 0;
        switch (CLUSTER_TYPE) {
            case OLD:
                return oldVal;
            case MIXED:
                if (false == masterIsNewVersion()) {
                    return oldVal;
                }
                if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                    return oldVal + 1;
                }
                return oldVal + 2;
            case UPGRADED:
                return oldVal + 3;
            default:
                throw new IllegalArgumentException("Unsupported cluster type [" + CLUSTER_TYPE + "]");
            }
    }

    private void assertAuditDocsExist() throws Exception {
        final String type = minVersionInCluster.before(Version.V_6_0_0) ? "event" : "doc";
        Response response = client().performRequest(new Request("GET", "/.security_audit_log*/" + type + "/_count"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull(responseMap.get("count"));
        assertThat((Integer) responseMap.get("count"), Matchers.greaterThanOrEqualTo(1));
    }

    private void assertNumUniqueNodeNameBuckets(int numBuckets) throws Exception {
        // call API that will hit all nodes
        Map<?, ?> nodesResponse = entityAsMap(client().performRequest(new Request("GET", "/_nodes/_all/info/version")));
        logger.info("all nodes {}", nodesResponse);

        Request aggRequest = new Request("GET", "/.security_audit_log*/_search");
        aggRequest.setJsonEntity(
                "{\n" +
                "    \"aggs\" : {\n" +
                "        \"nodes\" : {\n" +
                "            \"terms\" : { \"field\" : \"node_name\" }\n" +
                "        }\n" +
                "    }\n" +
                "}");
        aggRequest.addParameter("pretty", "true");
        Response aggResponse = client().performRequest(aggRequest);
        Map<String, Object> aggResponseMap = entityAsMap(aggResponse);
        logger.debug("aggResponse {}", aggResponseMap);
        Map<?, ?> aggregations = (Map<?, ?>) aggResponseMap.get("aggregations");
        assertNotNull(aggregations);
        Map<?, ?> nodesAgg = (Map<?, ?>) aggregations.get("nodes");
        assertNotNull(nodesAgg);
        List<?> buckets = (List<?>) nodesAgg.get("buckets");
        assertNotNull(buckets);
        assertThat("Found node buckets " + buckets, buckets, hasSize(numBuckets));
    }

    /**
     * Has the master been upgraded to the new version?
     */
    private boolean masterIsNewVersion() throws IOException {
        Map<?, ?> map = entityAsMap(client().performRequest(new Request("GET", "/_nodes/_master")));
        map = (Map<?, ?>) map.get("nodes");
        assertThat(map.values(), hasSize(1));
        map = (Map<?, ?>) map.values().iterator().next();
        Version masterVersion = Version.fromString(map.get("version").toString());
        return Version.CURRENT.equals(masterVersion);
    }
}
