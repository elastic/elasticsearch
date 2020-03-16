/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.Build;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class EqlActionIT extends AbstractEqlIntegTestCase {

    static final String indexPrefix = "endgame";
    static final String testIndexName = indexPrefix + "-1.4.0";
    protected static final String PARAM_FORMATTING = "%1$s.test -> %2$s";

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setUpData() throws Exception {
        // Insert test data
        ObjectMapper mapper = new ObjectMapper();
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, EqlActionIT.class.getResourceAsStream("/test_data.json"))) {
            List<Object> list = parser.list();
            for (Object item : list) {
                assertThat(item, instanceOf(HashMap.class));

                HashMap<String, Object> entry = (HashMap<String, Object>) item;

                // Adjust the structure of the document with additional event.category and @timestamp fields
                // Add event.category field
                HashMap<String, Object> objEvent = new HashMap<>();
                objEvent.put("category", entry.get("event_type"));
                entry.put("event", objEvent);

                // Add @timestamp field
                entry.put("@timestamp", entry.get("timestamp"));

                bulkBuilder.add(new IndexRequest(testIndexName).source(entry, XContentType.JSON));
            }
        }
        BulkResponse bulkResponse = bulkBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertThat(bulkResponse.hasFailures() ? bulkResponse.buildFailureMessage() : "", bulkResponse.hasFailures(), equalTo(false));

        ensureYellow(testIndexName);
    }

    @After
    public void tearDownData() {
        client().admin().indices().prepareDelete(testIndexName).get();
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {
        List<Object[]> testSpecs = new ArrayList<>();

        // Load EQL validation specs
        List<EqlSpec> specs = EqlSpecLoader.load("/test_queries.toml", true);
        List<EqlSpec> unsupportedSpecs = EqlSpecLoader.load("/test_queries_unsupported.toml", false);

        // Validate only currently supported specs
        int num = 1; // Seq number of the test
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
                testSpecs.add(new Object[]{num++, spec});
            }
        }
        return testSpecs;
    }

    private final int num;
    private final EqlSpec spec;

    public EqlActionIT(int num, EqlSpec spec) {
        this.num = num;
        this.spec = spec;
    }

    private static long[] extractIds(List<SearchHit> events) {
        final int len = events.size();
        final long ids[] = new long[len];
        for (int i = 0; i < len; i++) {
            ids[i] = ((Number) events.get(i).getSourceAsMap().get("serial_event_id")).longValue();
        }
        return ids;
    }

    private void assertSpec(List<SearchHit> events) {
        assertNotNull(events);
        assertArrayEquals("unexpected result for spec: [" + spec.toString() + "]", spec.expectedEventIds(), extractIds(events));
    }

    public final void test() {
        EqlSearchResponse response = new EqlSearchRequestBuilder(client(), EqlSearchAction.INSTANCE)
                .indices(testIndexName).query(spec.query()).get();

        assertSpec(response.hits().events());
    }
}
