/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.dlsfls;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class DocValuesSkipperFieldLevelSecurityRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final SecureString PASSWORD = new SecureString("test-user-password".toCharArray());
    private static final String INDEX = "logs-index";
    private static final String UNRESTRICTED_USER = "full_user";
    private static final String RESTRICTED_USER = "no_fields_user";
    private static final String TIMESTAMP_ONLY_USER = "timestamp_only_user";
    private static final long DAY = 86_400_000L;
    private static final long START = 1767225600000L;
    private static final int NUM_DOCS = 5;

    public void testSortAscendingOnHiddenTimestampReturnsMissingValues() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 3, "track_total_hits": true, "sort": [{"@timestamp": "asc"}]}""";

        final ObjectPath unrestricted = search(UNRESTRICTED_USER, body);
        assertThat(unrestricted.evaluate("hits.total.value"), equalTo(NUM_DOCS));
        assertThat(sortValues(unrestricted), equalTo(List.of(timestamp(1), timestamp(2), timestamp(3))));

        final ObjectPath restricted = search(RESTRICTED_USER, body);
        assertThat(restricted.evaluate("hits.total.value"), equalTo(NUM_DOCS));
        assertThat(sortValues(restricted), equalTo(List.of(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)));
    }

    public void testSortDescendingOnHiddenTimestampReturnsMissingValues() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 3, "track_total_hits": true, "sort": [{"@timestamp": "desc"}]}""";

        final ObjectPath unrestricted = search(UNRESTRICTED_USER, body);
        assertThat(unrestricted.evaluate("hits.total.value"), equalTo(NUM_DOCS));
        assertThat(sortValues(unrestricted), equalTo(List.of(timestamp(5), timestamp(4), timestamp(3))));

        final ObjectPath restricted = search(RESTRICTED_USER, body);
        assertThat(restricted.evaluate("hits.total.value"), equalTo(NUM_DOCS));
        assertThat(sortValues(restricted), equalTo(List.of(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE)));
    }

    public void testMaxAggregationOnHiddenTimestampReturnsNull() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = timestampAggregation("max");

        assertThat(search(UNRESTRICTED_USER, body).evaluate("aggregations.a.value"), equalTo((double) timestamp(5)));
        assertThat(search(RESTRICTED_USER, body).evaluate("aggregations.a.value"), nullValue());
    }

    public void testMinAggregationOnHiddenTimestampReturnsNull() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = timestampAggregation("min");

        assertThat(search(UNRESTRICTED_USER, body).evaluate("aggregations.a.value"), equalTo((double) timestamp(1)));
        assertThat(search(RESTRICTED_USER, body).evaluate("aggregations.a.value"), nullValue());
    }

    public void testAvgAggregationOnHiddenTimestampReturnsNull() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = timestampAggregation("avg");

        assertThat(search(UNRESTRICTED_USER, body).evaluate("aggregations.a.value"), equalTo((double) timestamp(3)));
        assertThat(search(RESTRICTED_USER, body).evaluate("aggregations.a.value"), nullValue());
    }

    public void testValueCountOnHiddenTimestampReturnsZero() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = timestampAggregation("value_count");

        assertThat(search(UNRESTRICTED_USER, body).evaluate("aggregations.a.value"), equalTo(NUM_DOCS));
        assertThat(search(RESTRICTED_USER, body).evaluate("aggregations.a.value"), equalTo(0));
    }

    public void testCardinalityOnHiddenTimestampReturnsZero() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = timestampAggregation("cardinality");

        assertThat(search(UNRESTRICTED_USER, body).evaluate("aggregations.a.value"), equalTo(NUM_DOCS));
        assertThat(search(RESTRICTED_USER, body).evaluate("aggregations.a.value"), equalTo(0));
    }

    public void testPercentilesOnHiddenTimestampReturnNull() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 0, "aggs": {"a": {"percentiles": {"field": "@timestamp", "percents": [25, 50, 75]}}}}""";

        final Map<String, Object> unrestricted = search(UNRESTRICTED_USER, body).evaluate("aggregations.a.values");
        for (String percent : List.of("25.0", "50.0", "75.0")) {
            assertThat(
                percent,
                ((Number) unrestricted.get(percent)).doubleValue(),
                allOf(greaterThanOrEqualTo((double) timestamp(1)), lessThanOrEqualTo((double) timestamp(5)))
            );
        }

        final Map<String, Object> restricted = search(RESTRICTED_USER, body).evaluate("aggregations.a.values");
        assertThat(restricted.keySet(), equalTo(Set.of("25.0", "50.0", "75.0")));
        assertThat(new ArrayList<>(restricted.values()), equalTo(Arrays.asList(null, null, null)));
    }

    public void testDateHistogramOnHiddenTimestampHasNoBuckets() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 0, "aggs": {"a": {"date_histogram": {"field": "@timestamp", "calendar_interval": "day"}}}}""";

        final ObjectPath unrestricted = search(UNRESTRICTED_USER, body);
        assertThat(unrestricted.evaluate("aggregations.a.buckets"), hasSize(1));
        assertThat(unrestricted.evaluate("aggregations.a.buckets.0.key_as_string"), equalTo("2026-01-01T00:00:00.000Z"));
        assertThat(unrestricted.evaluate("aggregations.a.buckets.0.doc_count"), equalTo(NUM_DOCS));

        assertThat(search(RESTRICTED_USER, body).evaluate("aggregations.a.buckets"), equalTo(List.of()));
    }

    public void testDocValueFieldsOmitsHiddenTimestamp() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 10, "_source": false, "docvalue_fields": ["@timestamp"]}""";

        assertThat(
            hitValues(search(UNRESTRICTED_USER, body), "fields.@timestamp.0"),
            containsInAnyOrder(
                "2026-01-01T00:00:01.000Z",
                "2026-01-01T00:00:02.000Z",
                "2026-01-01T00:00:03.000Z",
                "2026-01-01T00:00:04.000Z",
                "2026-01-01T00:00:05.000Z"
            )
        );
        assertThat(hitValues(search(RESTRICTED_USER, body), "fields"), equalTo(Collections.nCopies(NUM_DOCS, null)));
    }

    public void testScriptSeesNoHiddenTimestampValues() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 10, "_source": false, "script_fields": {"s": {"script": "doc['@timestamp'].size()"}}}""";

        assertThat(hitValues(search(UNRESTRICTED_USER, body), "fields.s.0"), equalTo(Collections.nCopies(NUM_DOCS, 1)));
        assertThat(hitValues(search(RESTRICTED_USER, body), "fields.s.0"), equalTo(Collections.nCopies(NUM_DOCS, 0)));
    }

    public void testTimestampRangeDoesNotMatchHiddenValues() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = Strings.format("""
            {"size": 0, "track_total_hits": true, "query": {"range": {"@timestamp": {"gte": %d}}}}""", timestamp(2));

        assertThat(search(UNRESTRICTED_USER, body).evaluate("hits.total.value"), equalTo(4));
        assertThat(search(RESTRICTED_USER, body).evaluate("hits.total.value"), equalTo(0));
    }

    public void testTimestampExistsDoesNotMatchHiddenValues() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 0, "track_total_hits": true, "query": {"exists": {"field": "@timestamp"}}}""";

        assertThat(search(UNRESTRICTED_USER, body).evaluate("hits.total.value"), equalTo(NUM_DOCS));
        assertThat(search(RESTRICTED_USER, body).evaluate("hits.total.value"), equalTo(0));
    }

    public void testSortOnHiddenKeywordReturnsMissingValues() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 3, "sort": [{"host.name": "asc"}]}""";

        assertThat(sortValues(search(UNRESTRICTED_USER, body)), equalTo(List.of("h-a", "h-b", "h-c")));
        assertThat(sortValues(search(RESTRICTED_USER, body)), equalTo(Arrays.asList(null, null, null)));
    }

    public void testTermsAggregationOnHiddenKeywordHasNoBuckets() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 0, "aggs": {"a": {"terms": {"field": "host.name"}}}}""";

        final List<Map<String, Object>> unrestricted = search(UNRESTRICTED_USER, body).evaluate("aggregations.a.buckets");
        assertThat(unrestricted.stream().map(bucket -> bucket.get("key")).toList(), equalTo(List.of("h-a", "h-b", "h-c", "h-d", "h-e")));

        assertThat(search(RESTRICTED_USER, body).evaluate("aggregations.a.buckets"), equalTo(List.of()));
    }

    public void testKeywordRangeDoesNotMatchHiddenValues() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 0, "track_total_hits": true, "query": {"range": {"host.name": {"gte": "h-b"}}}}""";

        assertThat(search(UNRESTRICTED_USER, body).evaluate("hits.total.value"), equalTo(4));
        assertThat(search(RESTRICTED_USER, body).evaluate("hits.total.value"), equalTo(0));
    }

    public void testGrantedTimestampStaysVisibleWhenHostNameIsHidden() throws IOException {
        createLogsIndex();
        createUsers();
        final String body = """
            {"size": 3, "_source": false, "sort": [{"@timestamp": "asc"}], "docvalue_fields": ["@timestamp", "host.name"]}""";
        final List<Long> firstTimestamps = List.of(timestamp(1), timestamp(2), timestamp(3));
        final List<Object> firstFormattedTimestamps = List.of(
            "2026-01-01T00:00:01.000Z",
            "2026-01-01T00:00:02.000Z",
            "2026-01-01T00:00:03.000Z"
        );

        final ObjectPath unrestricted = search(UNRESTRICTED_USER, body);
        assertThat(sortValues(unrestricted), equalTo(firstTimestamps));
        assertThat(hitValues(unrestricted, "fields.@timestamp.0"), equalTo(firstFormattedTimestamps));
        assertThat(fieldNames(unrestricted), equalTo(Collections.nCopies(3, Set.of("@timestamp", "host.name"))));

        final ObjectPath timestampOnly = search(TIMESTAMP_ONLY_USER, body);
        assertThat(sortValues(timestampOnly), equalTo(firstTimestamps));
        assertThat(hitValues(timestampOnly, "fields.@timestamp.0"), equalTo(firstFormattedTimestamps));
        assertThat(fieldNames(timestampOnly), equalTo(Collections.nCopies(3, Set.of("@timestamp"))));
    }

    public void testShardSkippingDoesNotDependOnHiddenValues() throws IOException {
        createFourShardLogsIndex();
        createUsers();
        final String matchingRange = timestampRange(START, START + DAY);
        final String nonMatchingRange = timestampRange(START - 10 * DAY, START - 9 * DAY);

        final ObjectPath unrestrictedMatching = preFilteredSearch(UNRESTRICTED_USER, matchingRange);
        final ObjectPath unrestrictedNonMatching = preFilteredSearch(UNRESTRICTED_USER, nonMatchingRange);
        assertThat(unrestrictedMatching.evaluate("hits.total.value"), equalTo(1));
        assertThat(unrestrictedMatching.evaluate("_shards.skipped"), equalTo(3));
        assertThat(unrestrictedNonMatching.evaluate("hits.total.value"), equalTo(0));
        assertThat(unrestrictedNonMatching.evaluate("_shards.skipped"), equalTo(4));

        final ObjectPath restrictedMatching = preFilteredSearch(RESTRICTED_USER, matchingRange);
        final ObjectPath restrictedNonMatching = preFilteredSearch(RESTRICTED_USER, nonMatchingRange);
        assertThat(restrictedMatching.evaluate("hits.total.value"), equalTo(0));
        assertThat(restrictedNonMatching.evaluate("hits.total.value"), equalTo(0));
        assertThat(
            "skipped shards must not depend on hidden values",
            restrictedMatching.evaluate("_shards.skipped"),
            equalTo(restrictedNonMatching.evaluate("_shards.skipped"))
        );
    }

    public void testEsqlRejectsHiddenTimestamp() throws IOException {
        createLogsIndex();
        createUsers();
        final Request esql = new Request("POST", "/_query");
        esql.setJsonEntity(Strings.format("""
            {"query": "FROM %s | STATS min_ts = MIN(@timestamp)"}""", INDEX));
        esql.setOptions(asUser(RESTRICTED_USER));

        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esql));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("@timestamp"));
    }

    private static long timestamp(int second) {
        return START + second * 1_000L;
    }

    private void createLogsIndex() throws IOException {
        // NOTE: the leaked skipper only misled readers on dense segments, so every document has @timestamp; skippers are enabled
        // explicitly because host.name only gets one on request
        createLogsdbIndex(1);
        final List<Integer> seconds = new ArrayList<>(IntStream.rangeClosed(1, NUM_DOCS).boxed().toList());
        Collections.shuffle(seconds, random());
        final StringBuilder bulk = new StringBuilder();
        for (int second : seconds) {
            bulk.append(Strings.format("""
                {"index": {}}
                {"@timestamp": %d, "host.name": "h-%s"}
                """, timestamp(second), (char) ('a' + second - 1)));
        }
        bulkIndex(bulk.toString());
    }

    private void createFourShardLogsIndex() throws IOException {
        createLogsdbIndex(4);
        final Map<Integer, String> routingByShard = routingPerShard(4);
        final StringBuilder bulk = new StringBuilder();
        for (Map.Entry<Integer, String> entry : routingByShard.entrySet()) {
            bulk.append(Strings.format("""
                {"index": {"routing": "%s"}}
                {"@timestamp": %d, "host.name": "h"}
                """, entry.getValue(), START + entry.getKey() * DAY));
        }
        bulkIndex(bulk.toString());
    }

    private Map<Integer, String> routingPerShard(int shards) throws IOException {
        final Map<Integer, String> routingByShard = new HashMap<>();
        for (int attempt = 0; attempt < 1_000 && routingByShard.size() < shards; attempt++) {
            final Request request = new Request("GET", "/" + INDEX + "/_search_shards");
            request.addParameter("routing", "r" + attempt);
            final int shard = assertOKAndCreateObjectPath(adminClient().performRequest(request)).evaluate("shards.0.0.shard");
            routingByShard.putIfAbsent(shard, "r" + attempt);
        }
        assertThat("routing values for every shard", routingByShard.size(), equalTo(shards));
        return routingByShard;
    }

    private void createLogsdbIndex(int shards) throws IOException {
        final Settings settings = Settings.builder()
            .put("index.mode", "logsdb")
            .put("index.mapping.use_doc_values_skipper", true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        createIndex(adminClient(), INDEX, settings, """
            "properties": {"@timestamp": {"type": "date"}, "host.name": {"type": "keyword"}}""");
    }

    private void bulkIndex(String body) throws IOException {
        final Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(body);
        assertThat(assertOKAndCreateObjectPath(adminClient().performRequest(bulk)).evaluate("errors"), equalTo(false));
    }

    private void createUsers() throws IOException {
        upsertRole(Strings.format("""
            {"indices": [{"names": ["%s"], "privileges": ["read"]}]}""", INDEX), "full_access");
        upsertRole(Strings.format("""
            {"indices": [{"names": ["%s"], "privileges": ["read"], "field_security": {"grant": []}}]}""", INDEX), "no_fields");
        createUser(UNRESTRICTED_USER, PASSWORD, List.of("full_access"));
        createUser(RESTRICTED_USER, PASSWORD, List.of("no_fields"));
        upsertRole(
            Strings.format("""
                {"indices": [{"names": ["%s"], "privileges": ["read"], "field_security": {"grant": ["@timestamp"]}}]}""", INDEX),
            "timestamp_only"
        );
        createUser(TIMESTAMP_ONLY_USER, PASSWORD, List.of("timestamp_only"));
    }

    private ObjectPath search(String user, String body) throws IOException {
        return search(user, body, Map.of());
    }

    private ObjectPath preFilteredSearch(String user, String body) throws IOException {
        return search(user, body, Map.of("pre_filter_shard_size", "1"));
    }

    private ObjectPath search(String user, String body, Map<String, String> parameters) throws IOException {
        final Request request = new Request("GET", "/" + INDEX + "/_search");
        parameters.forEach(request::addParameter);
        request.setJsonEntity(body);
        request.setOptions(asUser(user));
        final ObjectPath response = assertOKAndCreateObjectPath(client().performRequest(request));
        assertThat("failed shards", response.evaluate("_shards.failed"), equalTo(0));
        return response;
    }

    private static String timestampAggregation(String type) {
        return Strings.format("""
            {"size": 0, "aggs": {"a": {"%s": {"field": "@timestamp"}}}}""", type);
    }

    private static String timestampRange(long from, long to) {
        return Strings.format("""
            {"size": 0, "query": {"range": {"@timestamp": {"gte": %d, "lt": %d}}}}""", from, to);
    }

    private static List<Object> sortValues(ObjectPath response) throws IOException {
        return hitValues(response, "sort.0");
    }

    private static List<Object> hitValues(ObjectPath response, String path) throws IOException {
        final List<Map<String, Object>> hits = response.evaluate("hits.hits");
        final List<Object> values = new ArrayList<>();
        for (Map<String, Object> hit : hits) {
            values.add(ObjectPath.evaluate(hit, path));
        }
        return values;
    }

    private static List<Set<String>> fieldNames(ObjectPath response) throws IOException {
        final List<Set<String>> names = new ArrayList<>();
        for (Object fields : hitValues(response, "fields")) {
            names.add(((Map<?, ?>) fields).keySet().stream().map(String::valueOf).collect(Collectors.toSet()));
        }
        return names;
    }

    private static RequestOptions asUser(String user) {
        return RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, PASSWORD)).build();
    }
}
