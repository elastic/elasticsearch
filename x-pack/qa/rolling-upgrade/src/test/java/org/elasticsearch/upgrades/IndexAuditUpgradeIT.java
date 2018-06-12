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
import org.elasticsearch.common.Booleans;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class IndexAuditUpgradeIT extends AbstractUpgradeTestCase {

    public void testAuditLogs() throws Exception {
        assertBusy(() -> {
            assertAuditDocsExist();
            assertNumUniqueNodeNameBuckets(expectedNumUniqueNodeNameBuckets());
        });
    }

    private int expectedNumUniqueNodeNameBuckets() throws IOException {
        switch (CLUSTER_TYPE) {
            case OLD:
                // There are three nodes in the initial test cluster
                return 3;
            case MIXED:
                if (false == masterIsNewVersion()) {
                    return 3;
                }
                if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                    // One of the old nodes has been removed and we've added a new node
                    return 4;
                }
                // Two of the old nodes have been removed and we've added two new nodes
                return 5;
            case UPGRADED:
                return 6;
            default:
                throw new IllegalArgumentException("Unsupported cluster type [" + CLUSTER_TYPE + "]");
            }
    }

    private void assertAuditDocsExist() throws Exception {
        Response response = client().performRequest("GET", "/.security_audit_log*/doc/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> responseMap = entityAsMap(response);
        assertNotNull(responseMap.get("count"));
        assertThat((Integer) responseMap.get("count"), Matchers.greaterThanOrEqualTo(1));
    }

    private void assertNumUniqueNodeNameBuckets(int numBuckets) throws Exception {
        // call API that will hit all nodes
        Map<?, ?> nodesResponse = entityAsMap(client().performRequest("GET", "/_nodes/_all/info/version"));
        logger.info("all nodes {}", nodesResponse);

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
     * @throws IOException
     */
    private boolean masterIsNewVersion() throws IOException {
        Map<?, ?> map = entityAsMap(client().performRequest("GET", "/_nodes/_master"));
        map = (Map<?, ?>) map.get("nodes");
        assertThat(map.values(), hasSize(1));
        map = (Map<?, ?>) map.values().iterator().next();
        Version masterVersion = Version.fromString(map.get("version").toString());
        return Version.CURRENT.equals(masterVersion);
    }
}
