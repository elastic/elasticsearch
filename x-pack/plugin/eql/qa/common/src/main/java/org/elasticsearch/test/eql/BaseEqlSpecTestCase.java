/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql;

import org.elasticsearch.client.EqlClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.client.eql.EqlSearchResponse.Event;
import org.elasticsearch.client.eql.EqlSearchResponse.Hits;
import org.elasticsearch.client.eql.EqlSearchResponse.Sequence;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

public abstract class BaseEqlSpecTestCase extends ESRestTestCase {

    protected static final String PARAM_FORMATTING = "%1$s.test -> %2$s";

    private RestHighLevelClient highLevelClient;

    private final String index;
    private final String query;
    private final String name;
    private final long[] eventIds;
    private final boolean caseSensitive;

    @Before
    private void setup() throws Exception {
        if (client().performRequest(new Request("HEAD", "/" + index)).getStatusLine().getStatusCode() == 404) {
            DataLoader.loadDatasetIntoEs(highLevelClient(), this::createParser);
        }
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    protected static List<Object[]> asArray(List<EqlSpec> specs) {
        int counter = 0;
        List<Object[]> results = new ArrayList<>();

        for (EqlSpec spec : specs) {
            String name = spec.name();
            if (Strings.isNullOrEmpty(name)) {
                name = spec.note();
            }
            if (Strings.isNullOrEmpty(name)) {
                name = "" + (counter);
            }

            boolean[] values = spec.caseSensitive() == null ? new boolean[] { true, false } : new boolean[] { spec.caseSensitive() };

            for (boolean sensitive : values) {
                String prefixed = name + (sensitive ? "-sensitive" : "-insensitive");
                results.add(new Object[] { spec.query(), prefixed, spec.expectedEventIds(), sensitive });
            }
        }

        return results;
    }

    BaseEqlSpecTestCase(String index, String query, String name, long[] eventIds, boolean caseSensitive) {
        this.index = index;

        this.query = query;
        this.name = name;
        this.eventIds = eventIds;
        this.caseSensitive = caseSensitive;
    }

    public void test() throws Exception {
        assertResponse(runQuery(index, query, caseSensitive));
    }

    protected void assertResponse(EqlSearchResponse response) {
        Hits hits = response.hits();
        if (hits.events() != null) {
            assertEvents(hits.events());
        }
        else if (hits.sequences() != null) {
            assertSequences(hits.sequences());
        }
        else {
            fail("No events or sequences found");
        }
    }

    protected EqlSearchResponse runQuery(String index, String query, boolean isCaseSensitive) throws Exception {
        EqlSearchRequest request = new EqlSearchRequest(index, query);
        request.isCaseSensitive(isCaseSensitive);
        request.tiebreakerField("event.sequence");
        // some queries return more than 10 results
        request.size(50);
        request.fetchSize(randomIntBetween(2, 50));
        return runRequest(eqlClient(), request);
    }

    protected  EqlSearchResponse runRequest(EqlClient eqlClient, EqlSearchRequest request) throws IOException {
        return eqlClient.search(request, RequestOptions.DEFAULT);
    }

    protected EqlClient eqlClient() {
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

    protected void assertEvents(List<Event> events) {
        assertNotNull(events);
        logger.info("Events {}", events);
        long[] expected = eventIds;
        long[] actual = extractIds(events);
        assertArrayEquals(LoggerMessageFormat.format(null, "unexpected result for spec[{}] [{}] -> {} vs {}", name, query, Arrays.toString(
                expected), Arrays.toString(actual)),
                expected, actual);
    }

    @SuppressWarnings("unchecked")
    private long[] extractIds(List<Event> events) {
        final int len = events.size();
        final long[] ids = new long[len];
        for (int i = 0; i < len; i++) {
            Object field = events.get(i).sourceAsMap().get(sequenceField());
            ids[i] = ((Number) field).longValue();
        }
        return ids;
    }

    protected void assertSequences(List<Sequence> sequences) {
        List<Event> events = sequences.stream()
                .flatMap(s -> s.events().stream())
                .collect(toList());
        assertEvents(events);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Need to preserve data between parameterized tests runs
        return true;
    }

    protected abstract String sequenceField();
}
