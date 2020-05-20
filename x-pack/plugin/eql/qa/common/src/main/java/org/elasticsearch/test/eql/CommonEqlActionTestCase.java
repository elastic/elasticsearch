/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.EqlClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.client.eql.EqlSearchResponse.Hits;
import org.elasticsearch.client.eql.EqlSearchResponse.Sequence;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.instanceOf;

public abstract class CommonEqlActionTestCase extends ESRestTestCase {

    private RestHighLevelClient highLevelClient;

    static final String indexPrefix = "endgame";
    static final String testIndexName = indexPrefix + "-1.4.0";
    protected static final String PARAM_FORMATTING = "%1$s.test -> %2$s";

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    private static boolean isSetUp = false;
    private static int counter = 0;

    @SuppressWarnings("unchecked")
    private static void setupData(CommonEqlActionTestCase tc) throws Exception {
        if (isSetUp) {
            return;
        }

        CreateIndexRequest request = new CreateIndexRequest(testIndexName)
                .mapping(Streams.readFully(CommonEqlActionTestCase.class.getResourceAsStream("/mapping-default.json")),
                        XContentType.JSON);

        tc.highLevelClient().indices().create(request, RequestOptions.DEFAULT);

        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        try (XContentParser parser = tc.createParser(JsonXContent.jsonXContent,
                CommonEqlActionTestCase.class.getResourceAsStream("/test_data.json"))) {
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

                bulk.add(new IndexRequest(testIndexName).source(entry, XContentType.JSON));
            }
        }

        if (bulk.numberOfActions() > 0) {
            BulkResponse bulkResponse = tc.highLevelClient().bulk(bulk, RequestOptions.DEFAULT);
            assertEquals(RestStatus.OK, bulkResponse.status());
            assertFalse(bulkResponse.hasFailures());
            isSetUp = true;
        }
    }

    private static void cleanupData(CommonEqlActionTestCase tc) throws Exception {
        // Delete index after all tests ran
        if (isSetUp && (--counter == 0)) {
            deleteIndex(testIndexName);
            isSetUp = false;
        }
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Need to preserve data between parameterized tests runs
        return true;
    }

    @Before
    public void setup() throws Exception {
        setupData(this);
    }

    @After
    public void cleanup() throws Exception {
        cleanupData(this);
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {

        // Load EQL validation specs
        List<EqlSpec> specs = EqlSpecLoader.load("/test_queries.toml", true);
        specs.addAll(EqlSpecLoader.load("/test_queries_supported.toml", true));
        List<EqlSpec> unsupportedSpecs = EqlSpecLoader.load("/test_queries_unsupported.toml", false);

        // Validate only currently supported specs
        List<EqlSpec> filteredSpecs = new ArrayList<>();

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
                filteredSpecs.add(spec);
            }
        }
        counter = specs.size();
        return asArray(filteredSpecs);
    }

    public static List<Object[]> asArray(List<EqlSpec> specs) {
        AtomicInteger counter = new AtomicInteger();
        return specs.stream().map(spec -> {
            String name = spec.description();
            if (Strings.isNullOrEmpty(name)) {
                name = spec.note();
            }
            if (Strings.isNullOrEmpty(name)) {
                name = spec.query();
            }
            return new Object[] { counter.incrementAndGet(), name, spec };
        }).collect(toList());
    }

    private final int num;
    private final String name;
    private final EqlSpec spec;

    public CommonEqlActionTestCase(int num, String name, EqlSpec spec) {
        this.num = num;
        this.name = name;
        this.spec = spec;
    }

    public void test() throws Exception {
        assertResponse(runQuery(testIndexName, spec.query()));
    }

    protected void assertResponse(EqlSearchResponse response) {
        Hits hits = response.hits();
        if (hits.events() != null) {
            assertSearchHits(hits.events());
        }
        else if (hits.sequences() != null) {
            assertSequences(hits.sequences());
        }
        else {
            fail("No events or sequences found");
        }
    }

    protected EqlSearchResponse runQuery(String index, String query) throws Exception {
        EqlSearchRequest request = new EqlSearchRequest(testIndexName, query);
        return eqlClient().search(request, RequestOptions.DEFAULT);
    }

    protected EqlClient eqlClient() {
        return highLevelClient().eql();
    }

    private static long[] extractIds(List<SearchHit> events) {
        final int len = events.size();
        final long ids[] = new long[len];
        for (int i = 0; i < len; i++) {
            ids[i] = ((Number) events.get(i).getSourceAsMap().get("serial_event_id")).longValue();
        }
        return ids;
    }

    protected void assertSearchHits(List<SearchHit> events) {
        assertNotNull(events);
        assertArrayEquals("unexpected result for spec: [" + spec.toString() + "]", spec.expectedEventIds(), extractIds(events));
    }

    protected void assertSequences(List<Sequence> sequences) {
        List<SearchHit> events = sequences.stream()
                .flatMap(s -> s.events().stream())
                .collect(toList());
        assertSearchHits(events);
    }

    private RestHighLevelClient highLevelClient() {
        if (highLevelClient == null) {
            highLevelClient = new RestHighLevelClient(
                    client(),
                    ignore -> {
                    },
                    List.of()) {
            };
        }
        return highLevelClient;
    }
}
