/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.EqlClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.client.eql.EqlSearchResponse.Event;
import org.elasticsearch.client.eql.EqlSearchResponse.Hits;
import org.elasticsearch.client.eql.EqlSearchResponse.Sequence;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import static java.util.stream.Collectors.toList;

public abstract class BaseEqlSpecTestCase extends RemoteClusterAwareEqlRestTestCase {

    protected static final String PARAM_FORMATTING = "%2$s";

    private RestHighLevelClient highLevelClient;

    private final String index;
    private final String query;
    private final String name;
    private final long[] eventIds;

    @Before
    public void setup() throws Exception {
        RestClient provisioningClient = provisioningClient();
        if (provisioningClient.performRequest(new Request("HEAD", "/" + unqualifiedIndexName())).getStatusLine().getStatusCode() == 404) {
            DataLoader.loadDatasetIntoEs(highLevelClient(provisioningClient), this::createParser);
        }
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        try {
            provisioningAdminClient().performRequest(new Request("DELETE", "/*"));
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

            results.add(new Object[] { spec.query(), name, spec.expectedEventIds() });
        }

        return results;
    }

    BaseEqlSpecTestCase(String index, String query, String name, long[] eventIds) {
        this.index = index;

        this.query = query;
        this.name = name;
        this.eventIds = eventIds;
    }

    public void test() throws Exception {
        assertResponse(runQuery(index, query));
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

    protected EqlSearchResponse runQuery(String index, String query) throws Exception {
        EqlSearchRequest request = new EqlSearchRequest(index, query);

        request.eventCategoryField(eventCategory());
        request.timestampField(timestamp());
        String tiebreaker = tiebreaker();
        if (tiebreaker != null) {
            request.tiebreakerField(tiebreaker());
        }
        request.size(requestSize());
        request.fetchSize(requestFetchSize());
        request.resultPosition(requestResultPosition());
        return runRequest(eqlClient(), request);
    }

    protected  EqlSearchResponse runRequest(EqlClient eqlClient, EqlSearchRequest request) throws IOException {
        int timeout = Math.toIntExact(timeout().millis());

        RequestConfig config = RequestConfig.copy(RequestConfig.DEFAULT)
            .setConnectionRequestTimeout(timeout)
            .setConnectTimeout(timeout)
            .setSocketTimeout(timeout)
            .build();
        return eqlClient.search(request, RequestOptions.DEFAULT.toBuilder().setRequestConfig(config).build());
    }

    protected EqlClient eqlClient() {
        return highLevelClient().eql();
    }

    private RestHighLevelClient highLevelClient() {
        if (highLevelClient == null) {
            highLevelClient = highLevelClient(client());
        }
        return highLevelClient;
    }

    protected void assertEvents(List<Event> events) {
        assertNotNull(events);
        logger.debug("Events {}", new Object() {
            public String toString() {
                return eventsToString(events);
            }
        });

        long[] expected = eventIds;
        long[] actual = extractIds(events);
        assertArrayEquals(LoggerMessageFormat.format(null, "unexpected result for spec[{}] [{}] -> {} vs {}", name, query, Arrays.toString(
                expected), Arrays.toString(actual)),
                expected, actual);
    }

    private String eventsToString(List<Event> events) {
        StringJoiner sj = new StringJoiner(",", "[", "]");
        for (Event event : events) {
            sj.add(event.id() + "|" + event.index());
            sj.add(event.sourceAsMap().toString());
            sj.add("\n");
        }
        return sj.toString();
    }

    private long[] extractIds(List<Event> events) {
        final int len = events.size();
        final long[] ids = new long[len];
        for (int i = 0; i < len; i++) {
            Object field = events.get(i).sourceAsMap().get(tiebreaker());
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

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        return clientBuilder(settings, hosts);
    }

    protected String timestamp() {
        return "@timestamp";
    }

    protected String eventCategory() {
        return "event.category";
    }

    protected abstract String tiebreaker();

    protected int requestSize() {
        // some queries return more than 10 results
        return 50;
    }

    protected int requestFetchSize() {
        return randomIntBetween(2, requestSize());
    }

    protected String requestResultPosition() {
        return randomBoolean() ? "head" : "tail";
    }

    // strip any qualification from the received index string
    private String unqualifiedIndexName() {
        int offset = index.indexOf(':');
        return offset >= 0 ? index.substring(offset + 1) : index;
    }
}
