/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test.eql;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.TestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Loads EQL dataset into ES.
 *
 * Checks for predefined indices:
 * - endgame-140       - for existing data
 * - endgame-140-nanos - same as endgame-140, but with nano-precision timestamps
 * - extra             - additional data
 * - sample*         - data for "sample" functionality
 *
 * While the loader could be made generic, the queries are bound to each index and generalizing that would make things way too complicated.
 */
@SuppressWarnings("removal")
public class DataLoader {
    public static final String TEST_INDEX = "endgame-140";
    public static final String TEST_EXTRA_INDEX = "extra";
    public static final String TEST_NANOS_INDEX = "endgame-140-nanos";
    public static final String TEST_SAMPLE = "sample1,sample2,sample3";
    public static final String TEST_MISSING_EVENTS_INDEX = "missing-events";
    public static final String TEST_SAMPLE_MULTI = "sample-multi";

    private static final Map<String, String[]> replacementPatterns = Collections.unmodifiableMap(getReplacementPatterns());

    private static final long FILETIME_EPOCH_DIFF = 11644473600000L; // millis delta from the start of year 1601 (Windows filetime) to 1970
    private static final long FILETIME_ONE_MILLISECOND = 10 * 1000; // Windows filetime is in 100-nanoseconds ticks

    // runs as java main
    private static boolean main = false;

    private static Map<String, String[]> getReplacementPatterns() {
        final Map<String, String[]> map = Maps.newMapWithExpectedSize(1);
        map.put("[runtime_random_keyword_type]", new String[] { "keyword", "wildcard" });
        return map;
    }

    public static void main(String[] args) throws IOException {
        main = true;
        try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            loadDatasetIntoEs(new RestHighLevelClient(client, ignore -> {}, List.of()) {
            }, DataLoader::createParser);
        }
    }

    public static void loadDatasetIntoEs(
        RestHighLevelClient client,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p
    ) throws IOException {

        //
        // Main Index
        //
        load(client, TEST_INDEX, null, DataLoader::timestampToUnixMillis, p);
        //
        // Aux Index
        //
        load(client, TEST_EXTRA_INDEX, null, null, p);
        //
        // Date_Nanos index
        //
        // The data for this index is loaded from the same endgame-140.data sample, only having the mapping for @timestamp changed: the
        // chosen Windows filetime timestamps (2017+) can coincidentally also be readily used as nano-resolution unix timestamps (1973+).
        // There are mixed values with and without nanos precision so that the filtering is properly tested for both cases.
        load(client, TEST_NANOS_INDEX, TEST_INDEX, DataLoader::timestampToUnixNanos, p);
        load(client, TEST_SAMPLE, null, null, p);
        //
        // missing_events index
        //
        // load(client, TEST_MISSING_EVENTS_INDEX, null, null, p);
        load(client, TEST_SAMPLE_MULTI, null, null, p);
    }

    private static void load(
        RestHighLevelClient client,
        String indexNames,
        String dataName,
        Consumer<Map<String, Object>> datasetTransform,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p
    ) throws IOException {
        String[] splitNames = indexNames.split(",");
        for (String indexName : splitNames) {
            String name = "/data/" + indexName + ".mapping";
            URL mapping = DataLoader.class.getResource(name);
            if (mapping == null) {
                throw new IllegalArgumentException("Cannot find resource " + name);
            }
            name = "/data/" + (dataName != null ? dataName : indexName) + ".data";
            URL data = DataLoader.class.getResource(name);
            if (data == null) {
                throw new IllegalArgumentException("Cannot find resource " + name);
            }
            createTestIndex(client, indexName, readMapping(mapping));
            loadData(client, indexName, datasetTransform, data, p);
        }
    }

    private static void createTestIndex(RestHighLevelClient client, String indexName, String mapping) throws IOException {
        ESRestTestCase.createIndex(client.getLowLevelClient(), indexName, null, mapping, null);
    }

    /**
     * Reads the mapping file, ignoring comments and replacing placeholders for random types.
     */
    private static String readMapping(URL resource) throws IOException {
        try (BufferedReader reader = TestUtils.reader(resource)) {
            StringBuilder b = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#") == false) {
                    for (Entry<String, String[]> entry : replacementPatterns.entrySet()) {
                        line = line.replace(entry.getKey(), randomOf(entry.getValue()));
                    }
                    b.append(line);
                }
            }
            return b.toString();
        }
    }

    private static CharSequence randomOf(String... values) {
        return main ? values[0] : ESRestTestCase.randomFrom(values);
    }

    @SuppressWarnings("unchecked")
    private static void loadData(
        RestHighLevelClient client,
        String indexName,
        Consumer<Map<String, Object>> datasetTransform,
        URL resource,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p
    ) throws IOException {
        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        try (XContentParser parser = p.apply(JsonXContent.jsonXContent, TestUtils.inputStream(resource))) {
            List<Object> list = parser.list();
            for (Object item : list) {
                assertThat(item, instanceOf(Map.class));
                Map<String, Object> entry = (Map<String, Object>) item;
                if (datasetTransform != null) {
                    datasetTransform.accept(entry);
                }
                bulk.add(new IndexRequest(indexName).source(entry, XContentType.JSON));
            }
        }

        if (bulk.numberOfActions() > 0) {
            BulkResponse bulkResponse = client.bulk(bulk, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                LogManager.getLogger(DataLoader.class).info("Data loading FAILED");
            } else {
                LogManager.getLogger(DataLoader.class).info("Data loading OK");
            }
        }
    }

    private static void timestampToUnixMillis(Map<String, Object> entry) {
        Object object = entry.get("timestamp");
        assertThat(object, instanceOf(Long.class));
        Long ts = (Long) object;
        // currently this is windows filetime
        entry.put("@timestamp", winFileTimeToUnix(ts));
    }

    private static void timestampToUnixNanos(Map<String, Object> entry) {
        Object object = entry.get("timestamp");
        assertThat(object, instanceOf(Long.class));
        // interpret the value as nanos since the unix epoch
        String timestamp = object.toString();
        assertThat(timestamp.length(), greaterThan(12));
        // avoid double approximations and BigDecimal ops
        String millis = timestamp.substring(0, 12);
        String milliFraction = timestamp.substring(12);
        // strip the fractions right away if not actually present
        entry.put("@timestamp", milliFraction.equals("000000") ? millis : millis + "." + milliFraction);
        entry.put("timestamp", ((long) object) / 1_000_000L);
    }

    public static long winFileTimeToUnix(final long filetime) {
        long ts = (filetime / FILETIME_ONE_MILLISECOND);
        return ts - FILETIME_EPOCH_DIFF;
    }

    private static XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        NamedXContentRegistry contentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        return xContent.createParser(contentRegistry, LoggingDeprecationHandler.INSTANCE, data);
    }
}
