/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multiterms;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Runs slow aggregations with a timeout and asserts that they timeout and
 * cancel the queries.
 */
public class AggsTimeoutIT extends ESRestTestCase {
    private static final int DEPTH = 10;
    private static final int VALUE_COUNT = 4;
    private static final int TOTAL_DOCS = Math.toIntExact((long) Math.pow(VALUE_COUNT, DEPTH));
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(1);

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .plugin("x-pack-analytics")
        .module("aggregations")
        .jvmArg("-Xmx1g")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testTerms() throws Exception {
        Request request = new Request("POST", "/deep/_search");
        XContentBuilder body = JsonXContent.contentBuilder().prettyPrint().startObject();
        body.field("size", 0);
        agg(body, "terms", 10);
        request.setJsonEntity(Strings.toString(body.endObject()));
        setTimeout(request);
        try {
            Map<?, ?> response = responseAsMap(client().performRequest(request));
            assertMap("not expected to finish", response, matchesMap());
        } catch (SocketTimeoutException timeout) {
            logger.info("timed out");
            assertNoSearchesRunning();
        }
    }

    private void agg(XContentBuilder body, String type, int depth) throws IOException {
        if (depth == 0) {
            return;
        }
        body.startObject("aggs").startObject(field("agg", depth));
        {
            body.startObject(type);
            body.field("field", field("kwd", depth - 1));
            body.endObject();
        }
        agg(body, type, depth - 1);
        body.endObject().endObject();
    }

    public void testMultiTerms() throws Exception {
        Request request = new Request("POST", "/deep/_search");
        XContentBuilder body = JsonXContent.contentBuilder().prettyPrint().startObject();
        body.field("size", 0);
        autoDateInMultiTerms(body, b -> {
            for (int i = 0; i < DEPTH; i++) {
                b.startObject().field("field", field("kwd", i)).endObject();
            }
        });
        request.setJsonEntity(Strings.toString(body.endObject()));
        setTimeout(request);
        try {
            Map<?, ?> response = responseAsMap(client().performRequest(request));
            ListMatcher buckets = matchesList();
            for (int i = 0; i < 10; i++) {
                buckets = buckets.item(
                    matchesMap().entry("key_as_string", any(String.class))
                        .entry("key", hasSize(10))
                        .entry("doc_count", 1)
                        .entry("adh", matchesMap().entry("buckets", hasSize(1)).entry("interval", "1s"))
                );
            }
            MapMatcher agg = matchesMap().entry("buckets", buckets)
                .entry("doc_count_error_upper_bound", 0)
                .entry("sum_other_doc_count", greaterThan(0));
            assertMap(response, matchesMap().extraOk().entry("aggregations", matchesMap().entry("multi", agg)));
        } catch (SocketTimeoutException timeout) {
            logger.info("timed out");
            assertNoSearchesRunning();
        }
    }

    public void testMultiTermWithTimestamp() throws Exception {
        Request request = new Request("POST", "/deep/_search");
        XContentBuilder body = JsonXContent.contentBuilder().prettyPrint().startObject();
        body.field("size", 0);
        autoDateInMultiTerms(body, b -> {
            b.startObject().field("field", field("kwd", 0)).endObject();
            b.startObject().field("field", "@timestamp").endObject();
        });
        request.setJsonEntity(Strings.toString(body.endObject()));
        setTimeout(request);
        try {
            Map<?, ?> response = responseAsMap(client().performRequest(request));
            ListMatcher buckets = matchesList();
            for (int i = 0; i < 10; i++) {
                buckets = buckets.item(
                    matchesMap().entry("key_as_string", any(String.class))
                        .entry("key", hasSize(10))
                        .entry("doc_count", 1)
                        .entry("adh", matchesMap().entry("buckets", hasSize(1)).entry("interval", "1s"))
                );
            }
            MapMatcher agg = matchesMap().entry("buckets", buckets)
                .entry("doc_count_error_upper_bound", 0)
                .entry("sum_other_doc_count", greaterThan(0));
            assertMap(response, matchesMap().extraOk().entry("aggregations", matchesMap().entry("multi", agg)));
        } catch (SocketTimeoutException timeout) {
            logger.info("timed out");
            assertNoSearchesRunning();
        }
    }

    private void autoDateInMultiTerms(XContentBuilder body, CheckedConsumer<XContentBuilder, IOException> terms) throws IOException {
        body.startObject("aggs").startObject("multi");
        {
            body.startObject("multi_terms");
            {
                body.startArray("terms");
                terms.accept(body);
                body.endArray();
                body.startArray("order");
                {
                    body.startObject().field("_count", "desc").endObject();
                    body.startObject().field("_key", "asc").endObject();
                }
                body.endArray();
            }
            body.endObject();
            body.startObject("aggs").startObject("adh").startObject("auto_date_histogram");
            {
                body.field("field", "@timestamp");
                body.field("buckets", 1);
            }
            body.endObject().endObject().endObject();
        }
        body.endObject().endObject();
    }

    @Before
    public void createDeep() throws IOException {
        if (indexExists("deep")) {
            return;
        }
        logger.info("creating deep index");
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        mapping.startObject("@timestamp").field("type", "date").endObject();
        for (int f = 0; f < DEPTH; f++) {
            mapping.startObject(field("kwd", f)).field("type", "keyword").endObject();
        }
        CreateIndexResponse createIndexResponse = createIndex(
            "deep",
            Settings.builder().put("index.number_of_replicas", 0).build(),
            Strings.toString(mapping.endObject().endObject())
        );
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
        Bulk bulk = new Bulk();
        bulk.doc(new StringBuilder("{"), 0);
        bulk.flush();

        MapMatcher shardsOk = matchesMap().entry("total", 1).entry("failed", 0).entry("successful", 1);
        logger.info("refreshing deep index");
        Map<?, ?> refresh = responseAsMap(client().performRequest(new Request("POST", "/_refresh")));
        assertMap(refresh, matchesMap().entry("_shards", shardsOk));

        logger.info("double checking deep index count");
        Map<?, ?> count = responseAsMap(client().performRequest(new Request("POST", "/deep/_count")));
        assertMap(count, matchesMap().entry("_shards", shardsOk.entry("skipped", 0)).entry("count", TOTAL_DOCS));

        logger.info("deep index ready for test");
    }

    private String field(String prefix, int field) {
        return String.format(Locale.ROOT, "%s%03d", prefix, field);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    class Bulk {
        private static final int BULK_SIZE = Math.toIntExact(ByteSizeValue.ofMb(2).getBytes());

        StringBuilder bulk = new StringBuilder();
        int current = 0;
        int total = 0;
        long timestamp = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-01-01T00:00:00Z");

        void doc(StringBuilder doc, int field) throws IOException {
            if (field != 0) {
                doc.append(',');
            }
            int len = doc.length();
            for (int value = 0; value < VALUE_COUNT; value++) {
                doc.append('"').append(field("kwd", field)).append("\":\"").append(value).append('"');
                if (field == DEPTH - 1) {
                    doc.append(",\"@timestamp\":").append(timestamp).append('}');
                    timestamp += TimeValue.timeValueMinutes(1).millis();
                    addToBulk(doc);
                } else {
                    doc(doc, field + 1);
                }
                doc.setLength(len);
            }
        }

        void addToBulk(StringBuilder doc) throws IOException {
            current++;
            total++;
            bulk.append("{\"index\":{}}\n");
            bulk.append(doc).append('\n');
            if (bulk.length() > BULK_SIZE) {
                flush();
            }
        }

        void flush() throws IOException {
            logger.info(
                "Flushing to deep {} docs/{}. Total {}% {}/{}",
                current,
                ByteSizeValue.ofBytes(bulk.length()),
                String.format(Locale.ROOT, "%04.1f", 100.0 * total / TOTAL_DOCS),
                total,
                TOTAL_DOCS
            );
            Request request = new Request("POST", "/deep/_bulk");
            request.setJsonEntity(bulk.toString());
            Map<?, ?> response = responseAsMap(client().performRequest(request));
            assertMap(response, matchesMap().extraOk().entry("errors", false));
            bulk.setLength(0);
            current = 0;
        }
    }

    private void setTimeout(Request request) {
        RequestConfig.Builder config = RequestConfig.custom();
        config.setSocketTimeout(Math.toIntExact(TIMEOUT.millis()));
        request.setOptions(request.getOptions().toBuilder().setRequestConfig(config.build()));
    }

    /**
     * Asserts that within a minute the _search has left the _tasks api.
     * <p>
     *     It'd sure be more convenient if, whenever the _search has returned
     *     back to us the _tasks API doesn't contain the _search. But sometimes
     *     it still does. So long as it stops <strong>eventually</strong> that's
     *     still indicative of the interrupt code working.
     * </p>
     */
    private void assertNoSearchesRunning() throws Exception {
        assertBusy(() -> {
            Request tasks = new Request("GET", "/_tasks");
            tasks.addParameter("actions", "*search");
            tasks.addParameter("detailed", "");
            assertBusy(() -> {
                Map<?, ?> response = responseAsMap(client().performRequest(tasks));
                // If there are running searches the map in `nodes` is non-empty.
                if (response.isEmpty() == false) {
                    logger.warn("search still running, hot threads:\n{}", hotThreads());
                }
                assertMap(response, matchesMap().entry("nodes", matchesMap()));
            });
        }, 1, TimeUnit.MINUTES);
    }

    private String hotThreads() throws IOException {
        Request tasks = new Request("GET", "/_nodes/hot_threads");
        return EntityUtils.toString(client().performRequest(tasks).getEntity());
    }
}
