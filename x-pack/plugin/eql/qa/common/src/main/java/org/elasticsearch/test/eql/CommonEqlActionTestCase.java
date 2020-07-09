/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.client.EqlClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.client.eql.EqlSearchResponse.Hits;
import org.elasticsearch.client.eql.EqlSearchResponse.Sequence;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.test.eql.DataLoader.testIndexName;

public abstract class CommonEqlActionTestCase extends ESRestTestCase {

    protected static final String PARAM_FORMATTING = "%1$s.test -> %2$s";
    private static int counter = 0;
    private RestHighLevelClient highLevelClient;

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    @Before
    public void setup() throws Exception {
        if (client().performRequest(new Request("HEAD", "/" + testIndexName)).getStatusLine().getStatusCode() == 404) {
            DataLoader.loadDatasetIntoEs(highLevelClient(), (t, u) -> createParser(t, u));
        }
    }

    @After
    public void cleanup() throws Exception {
        if (--counter == 0) {
            deleteIndex(testIndexName);
        }
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {

        // Load EQL validation specs
        Set<String> uniqueTestNames = new HashSet<>();
        List<EqlSpec> specs = EqlSpecLoader.load("/test_queries.toml", true, uniqueTestNames);
        specs.addAll(EqlSpecLoader.load("/additional_test_queries.toml", true, uniqueTestNames));
        List<EqlSpec> unsupportedSpecs = EqlSpecLoader.load("/test_queries_unsupported.toml", false, uniqueTestNames);

        // Validate only currently supported specs
        List<EqlSpec> filteredSpecs = new ArrayList<>();

        for (EqlSpec spec : specs) {
            boolean supported = true;
            // Check if spec is supported, simple iteration, cause the list is short.
            for (EqlSpec unSpec : unsupportedSpecs) {
                if (spec.equals(unSpec)) {
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

    private static List<Object[]> asArray(List<EqlSpec> specs) {
        AtomicInteger counter = new AtomicInteger();
        return specs.stream().map(spec -> {
            String name = spec.name();
            if (Strings.isNullOrEmpty(name)) {
                name = spec.note();
            }
            if (Strings.isNullOrEmpty(name)) {
                name = "" + (counter.get() + 1);
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
        // run both tests if case sensitivity doesn't matter
        if (spec.caseSensitive() == null) {
            assertResponse(runQuery(testIndexName, spec.query(), true));
            assertResponse(runQuery(testIndexName, spec.query(), false));
        }
        // run only the case sensitive test
        else if (spec.caseSensitive()) {
            assertResponse(runQuery(testIndexName, spec.query(), true));
        }
        // run only the case insensitive test
        else {
            assertResponse(runQuery(testIndexName, spec.query(), false));
        }
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

    protected EqlSearchResponse runQuery(String index, String query, boolean isCaseSensitive) throws Exception {
        EqlSearchRequest request = new EqlSearchRequest(testIndexName, query);
        request.isCaseSensitive(isCaseSensitive);
        request.tiebreakerField("event.sequence");
        // some queries return more than 10 results
        request.size(50);
        request.fetchSize(randomIntBetween(2, 50));
        return eqlClient().search(request, RequestOptions.DEFAULT);
    }

    private EqlClient eqlClient() {
        return highLevelClient().eql();
    }

    private RestHighLevelClient highLevelClient() {
        if (highLevelClient == null) {
            highLevelClient = new RestHighLevelClient(
                    client(),
                    ignore -> {
                    },
                    Collections.emptyList()) {
            };
        }
        return highLevelClient;
    }

    protected void assertSearchHits(List<SearchHit> events) {
        assertNotNull(events);
        long[] expected = spec.expectedEventIds();
        long[] actual = extractIds(events);
        assertArrayEquals("unexpected result for spec: [" + spec.toString() + "]" + Arrays.toString(expected) + " vs " + Arrays.toString(
                actual), expected, actual);
    }

    private static long[] extractIds(List<SearchHit> events) {
        final int len = events.size();
        final long ids[] = new long[len];
        for (int i = 0; i < len; i++) {
            ids[i] = ((Number) events.get(i).getSourceAsMap().get("serial_event_id")).longValue();
        }
        return ids;
    }

    protected void assertSequences(List<Sequence> sequences) {
        List<SearchHit> events = sequences.stream()
                .flatMap(s -> s.events().stream())
                .collect(toList());
        assertSearchHits(events);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Need to preserve data between parameterized tests runs
        return true;
    }
}
