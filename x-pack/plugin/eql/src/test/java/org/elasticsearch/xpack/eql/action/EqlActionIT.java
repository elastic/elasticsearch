/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.Build;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.junit.BeforeClass;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class EqlActionIT extends AbstractEqlIntegTestCase {

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    public void testEqlSearchAction() throws Exception {
        final String indexPrefix = "endgame";
        final String testIndexName = indexPrefix + "-1.4.0";

        // Insert test data
        ObjectMapper mapper = new ObjectMapper();
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        JsonNode rootNode = mapper.readTree(this.getClass().getResourceAsStream("/test_data.json"));
        Iterator<JsonNode> entries = rootNode.elements();
        while (entries.hasNext()) {
            JsonNode entry = entries.next();
            bulkBuilder.add(new IndexRequest(testIndexName).source(entry.toString(), XContentType.JSON));
        }
        BulkResponse bulkResponse = bulkBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertThat(bulkResponse.hasFailures() ? bulkResponse.buildFailureMessage() : "", bulkResponse.hasFailures(), equalTo(false));

        ensureYellow(testIndexName);

        // Load EQL validation specs
        List<EqlSpec> specs = EqlSpecLoader.load("/test_queries.toml", true);
        List<EqlSpec> unsupportedSpecs = EqlSpecLoader.load("/test_queries_unsupported.toml", false);

        // Validate only currently supported specs
        for (EqlSpec spec : specs) {
            boolean supported = true;
            // Check if spec is supported, simple iteration, cause the list is short.
            for (EqlSpec unSpec : unsupportedSpecs) {
                if (spec.query() != null && spec.query().equals(unSpec.query())) {
                    supported = false;
                    break;
                }
            }

            if (supported) {
                logger.info("execute: " + spec.query());
                EqlSearchResponse response = new EqlSearchRequestBuilder(client(), EqlSearchAction.INSTANCE)
                    .indices(testIndexName).rule(spec.query()).get();

                List<SearchHit> events = response.hits().events();
                assertNotNull(events);

                final int len = events.size();
                final long ids[] = new long[len];
                for (int i = 0; i < events.size(); i++) {
                    ids[i] = events.get(i).docId();
                }
                final String msg = "unexpected result for spec: [" + spec.toString() + "]";
                assertArrayEquals(msg, spec.expectedEventIds(), ids);
            }
        }
    }
}
