/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class BaseEqlSpecTestCase extends RemoteClusterAwareEqlRestTestCase {

    protected static final String PARAM_FORMATTING = "%2$s";

    private final String index;
    private final String query;
    private final String name;
    private final List<long[]> eventIds;
    /**
     * Join keys can be of multiple types, but toml is very restrictive and doesn't allow mixed types values in the same array of values
     * For now, every value will be converted to a String.
     */
    private final String[] joinKeys;

    /**
     * any negative value means undefined (ie. no "size" will be passed to the query)
     */
    private final int size;
    private final int maxSamplesPerKey;

    @Before
    public void setup() throws Exception {
        RestClient provisioningClient = provisioningClient();
        boolean dataLoaded = Arrays.stream(index.split(","))
            .anyMatch(
                indexName -> doWithRequest(
                    new Request("HEAD", "/" + unqualifiedIndexName(indexName)),
                    provisioningClient,
                    response -> response.getStatusLine().getStatusCode() == 200
                )
            );

        if (dataLoaded == false) {
            DataLoader.loadDatasetIntoEs(highLevelClient(provisioningClient), this::createParser);
        }
    }

    private static boolean doWithRequest(Request request, RestClient client, Function<Response, Boolean> consumer) {
        try {
            return consumer.apply(client.performRequest(request));
        } catch (IOException e) {
            throw new RuntimeException(e);
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

            results.add(
                new Object[] { spec.query(), name, spec.expectedEventIds(), spec.joinKeys(), spec.size(), spec.maxSamplesPerKey() }
            );
        }

        return results;
    }

    BaseEqlSpecTestCase(
        String index,
        String query,
        String name,
        List<long[]> eventIds,
        String[] joinKeys,
        Integer size,
        Integer maxSamplesPerKey
    ) {
        this.index = index;

        this.query = query;
        this.name = name;
        this.eventIds = eventIds;
        this.joinKeys = joinKeys;
        this.size = size == null ? -1 : size;
        this.maxSamplesPerKey = maxSamplesPerKey == null ? -1 : maxSamplesPerKey;
    }

    public void test() throws Exception {
        assertResponse(runQuery(index, query));
    }

    private void assertResponse(ObjectPath response) throws Exception {
        List<Map<String, Object>> events = response.evaluate("hits.events");
        List<Map<String, Object>> sequences = response.evaluate("hits.sequences");

        if (events != null) {
            assertEvents(events);
        } else if (sequences != null) {
            assertSequences(sequences);
        } else {
            fail("No events or sequences found");
        }
    }

    protected ObjectPath runQuery(String index, String query) throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("query", query);
        builder.field("event_category_field", eventCategory());
        builder.field("timestamp_field", timestamp());
        String tiebreaker = tiebreaker();
        if (tiebreaker != null) {
            builder.field("tiebreaker_field", tiebreaker);
        }
        builder.field("size", this.size < 0 ? requestSize() : this.size);
        builder.field("fetch_size", requestFetchSize());
        builder.field("result_position", requestResultPosition());
        if (maxSamplesPerKey > 0) {
            builder.field("max_samples_per_key", maxSamplesPerKey);
        }
        builder.endObject();

        Request request = new Request("POST", "/" + index + "/_eql/search");
        Boolean ccsMinimizeRoundtrips = ccsMinimizeRoundtrips();
        if (ccsMinimizeRoundtrips != null) {
            request.addParameter("ccs_minimize_roundtrips", ccsMinimizeRoundtrips.toString());
        }
        int timeout = Math.toIntExact(timeout().millis());
        RequestConfig config = RequestConfig.copy(RequestConfig.DEFAULT)
            .setConnectionRequestTimeout(timeout)
            .setConnectTimeout(timeout)
            .setSocketTimeout(timeout)
            .build();
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        request.setOptions(optionsBuilder.setRequestConfig(config).build());
        request.setJsonEntity(Strings.toString(builder));
        return ObjectPath.createFromResponse(client().performRequest(request));
    }

    private void assertEvents(List<Map<String, Object>> events) {
        assertNotNull(events);
        logger.debug("Events {}", new Object() {
            public String toString() {
                return eventsToString(events);
            }
        });

        long[] actual = extractIds(events);
        if (eventIds.size() == 1) {
            long[] expected = eventIds.get(0);
            assertArrayEquals(
                LoggerMessageFormat.format(
                    null,
                    "unexpected result for spec[{}] [{}] -> {} vs {}",
                    name,
                    query,
                    Arrays.toString(expected),
                    Arrays.toString(actual)
                ),
                expected,
                actual
            );
        } else {
            boolean succeeded = false;
            for (long[] expected : eventIds) {
                if (Arrays.equals(expected, actual)) {
                    succeeded = true;
                    break;
                }
            }
            if (succeeded == false) {
                String msg = LoggerMessageFormat.format(
                    null,
                    "unexpected result for spec[{}] [{}]. Found: {} - Expected one of the following: {}",
                    name,
                    query,
                    Arrays.toString(actual),
                    eventIds.stream().map(Arrays::toString).collect(Collectors.joining(", "))
                );
                fail(msg);
            }
        }

    }

    private static String eventsToString(List<Map<String, Object>> events) {
        StringJoiner sj = new StringJoiner(",", "[", "]");
        for (Map<String, Object> event : events) {
            sj.add(event.get("_id") + "|" + event.get("_index"));
            sj.add(event.get("_source").toString());
            sj.add("\n");
        }
        return sj.toString();
    }

    @SuppressWarnings("unchecked")
    private long[] extractIds(List<Map<String, Object>> events) {
        final int len = events.size();
        final long[] ids = new long[len];
        for (int i = 0; i < len; i++) {
            Map<String, Object> event = events.get(i);
            Map<String, Object> source = (Map<String, Object>) event.get("_source");
            if (source == null) {
                ids[i] = -1;
            } else {
                Object field = source.get(idField());
                ids[i] = ((Number) field).longValue();
            }
        }
        return ids;
    }

    @SuppressWarnings("unchecked")
    private void assertSequences(List<Map<String, Object>> sequences) {
        List<Map<String, Object>> events = sequences.stream()
            .flatMap(s -> ((List<Map<String, Object>>) s.getOrDefault("events", Collections.emptyList())).stream())
            .toList();
        assertEvents(events);
        List<Object> keys = sequences.stream()
            .flatMap(s -> ((List<Object>) s.getOrDefault("join_keys", Collections.emptyList())).stream())
            .toList();
        assertJoinKeys(keys);
    }

    private void assertJoinKeys(List<Object> keys) {
        logger.debug("Join keys {}", new Object() {
            public String toString() {
                return keysToString(keys);
            }
        });

        if (joinKeys == null || joinKeys.length == 0) {
            return;
        }
        String[] actual = new String[keys.size()];
        int i = 0;
        for (Object key : keys) {
            if (key == null) {
                actual[i] = "null";
            } else {
                actual[i] = key.toString();
            }
            i++;
        }
        assertArrayEquals(
            LoggerMessageFormat.format(
                null,
                "unexpected result for spec[{}] [{}] -> {} vs {}",
                name,
                query,
                Arrays.toString(joinKeys),
                Arrays.toString(actual)
            ),
            joinKeys,
            actual
        );
    }

    private static String keysToString(List<Object> keys) {
        StringJoiner sj = new StringJoiner(",", "[", "]");
        for (Object key : keys) {
            sj.add(key.toString());
            sj.add("\n");
        }
        return sj.toString();
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

    protected String idField() {
        return tiebreaker();
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
    private static String unqualifiedIndexName(String indexName) {
        int offset = indexName.indexOf(':');
        return offset >= 0 ? indexName.substring(offset + 1) : indexName;
    }
}
