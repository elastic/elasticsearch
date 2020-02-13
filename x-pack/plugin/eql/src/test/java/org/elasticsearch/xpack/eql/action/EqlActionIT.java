/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class EqlActionIT extends AbstractEqlIntegTestCase {

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    public void testEqlSearchAction() throws Exception {
        final String indexPrefix = "endgame";
        final String testIndexName = indexPrefix + "-1.4.0";
        final String testIndexPattern = indexPrefix + "-*";

        String endgame = copyToStringFromClasspath("/endgame.json");
        endgame = endgame.replace("[index_pattern_placeholder]", testIndexPattern);

        assertAcked(client().admin().indices().preparePutTemplate(testIndexName)
            .setSource(endgame.getBytes(StandardCharsets.UTF_8), XContentType.JSON).get());

        // Insert test data
        InputStream is = EqlActionIT.class.getResourceAsStream("/endgame.dat");
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(is, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                bulkBuilder.add(new IndexRequest(testIndexName).source(line.trim(), XContentType.JSON));
            }
            BulkResponse response = bulkBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            assertThat(response.hasFailures() ? response.buildFailureMessage() : "", response.hasFailures(), equalTo(false));
        }

        ensureYellow(testIndexName);

        // Load EQL validation specs
        List<EqlSpec> specs = EqlSpecLoader.load("/test_queries.toml", true);
        List<EqlSpec> unsupportedSpecs = EqlSpecLoader.load("/test_queries_unsupported.toml", false);

        // Validate only currently supported specs
        for (EqlSpec spec : specs) {
            boolean supported = true;
            // Check if spec is supported, simple iteration, cause the list is short.
            for (EqlSpec unSpec : unsupportedSpecs) {
                if (spec.query.equals(unSpec.query)) {
                    supported = false;
                    break;
                }
            }

            if (supported) {
                logger.info("execute: " + spec.query);
                EqlSearchResponse response = new EqlSearchRequestBuilder(client(), EqlSearchAction.INSTANCE)
                    .indices(testIndexName).rule(spec.query).get();

                List<SearchHit> events = response.hits().events();
                assertNotNull(events);

                final int len = events.size();
                final int ids[] = new int[len];
                for (int i = 0; i < events.size(); i++) {
                    ids[i] = events.get(i).docId();
                }
                final String msg = "unexpected result for spec: [" + spec.toString() + "]";
                assertArrayEquals(msg, spec.expectedEventIds, ids);
            }
        }
    }
}
