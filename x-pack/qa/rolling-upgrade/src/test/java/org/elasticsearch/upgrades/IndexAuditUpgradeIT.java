/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IndexAuditUpgradeIT extends AbstractUpgradeTestCase {

    public void testDocsAuditedInOldCluster() throws Exception {
        assumeTrue("only runs against old cluster", clusterType == CLUSTER_TYPE.OLD);
        assertBusy(() -> {
            assertAuditDocsExist();
            assertNumUniqueNodeNameBuckets(2);
        });
    }

    public void testDocsAuditedInMixedCluster() throws Exception {
        assumeTrue("only runs against mixed cluster", clusterType == CLUSTER_TYPE.MIXED);
        assertBusy(() -> {
            assertAuditDocsExist();
            assertNumUniqueNodeNameBuckets(2);
        });
    }

    public void testDocsAuditedInUpgradedCluster() throws Exception {
        assumeTrue("only runs against upgraded cluster", clusterType == CLUSTER_TYPE.UPGRADED);
        assertBusy(() -> {
            assertAuditDocsExist();
            assertNumUniqueNodeNameBuckets(4);
        });
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
        assertEquals("Found node buckets " + buckets, numBuckets, buckets.size());
    }
}
