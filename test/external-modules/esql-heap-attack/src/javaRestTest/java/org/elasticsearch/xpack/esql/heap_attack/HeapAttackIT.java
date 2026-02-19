/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.matchesRegex;

/**
 * Tests that run ESQL queries that use a ton of memory. We want to make
 * sure they don't consume the entire heap and crash Elasticsearch.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackIT extends HeapAttackTestCase {

    /**
     * This used to fail, but we've since compacted top n so it actually succeeds now.
     */
    public void testSortByManyLongsSuccess() throws IOException {
        initManyLongs(10);
        // | SORT a, b, i0, i1, ...i500 | KEEP a, b | LIMIT 10000
        Map<String, Object> response = sortByManyLongs(500);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "b").entry("type", "long"));
        ListMatcher values = matchesList();
        for (int b = 0; b < 10; b++) {
            for (int i = 0; i < 1000; i++) {
                values = values.item(List.of(0, b));
            }
        }
        assertResultMap(response, columns, values);
    }

    /**
     * This used to crash the node with an out of memory, but now it just trips a circuit breaker.
     */
    public void testSortByManyLongsTooMuchMemory() throws IOException {
        initManyLongs(10);
        // 5000 is plenty to break on most nodes
        assertCircuitBreaks(attempt -> sortByManyLongs(attempt * 5000));
    }

    /**
     * This should record an async response with a {@link CircuitBreakingException}.
     */
    public void testSortByManyLongsTooMuchMemoryAsync() throws IOException {
        initManyLongs(10);
        Request request = new Request("POST", "/_query/async");
        request.addParameter("error_trace", "");
        request.setJsonEntity(makeSortByManyLongs(5000).toString().replace("\n", "\\n"));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(6).millis())).build())
                .setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        logger.info("--> test {} started async", getTestName());
        Response response = runQuery(() -> {
            Response r = client().performRequest(request);
            Map<?, ?> map = responseAsMap(r);
            assertMap(map, matchesMap().extraOk().entry("is_running", true).entry("id", any(String.class)));
            String id = map.get("id").toString();
            Request fetch = new Request("GET", "/_query/async/" + id);
            long endTime = System.nanoTime() + TimeValue.timeValueMinutes(5).nanos();
            while (System.nanoTime() < endTime) {
                Response resp;
                try {
                    resp = client().performRequest(fetch);
                } catch (ResponseException e) {
                    if (e.getResponse().getStatusLine().getStatusCode() == 403) {
                        /*
                         * There's a bug when loading from the translog with security
                         * enabled. If we retry a few times we'll load from the index
                         * itself and should succeed.
                         */
                        logger.error("polled for results got 403");
                        continue;
                    }
                    if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                        logger.error("polled for results got 404");
                        continue;
                    }
                    if (e.getResponse().getStatusLine().getStatusCode() == 503) {
                        logger.error("polled for results got 503");
                        continue;
                    }
                    if (e.getResponse().getStatusLine().getStatusCode() == 429) {
                        // This is what we were going to for - a CircuitBreakerException
                        return e.getResponse();
                    }
                    throw e;
                }
                Map<?, ?> m = responseAsMap(resp);
                logger.error("polled for results {}", m);
                boolean isRunning = (boolean) m.get("is_running");
                if (isRunning == false) {
                    return resp;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            throw new IOException("timed out");
        });
        assertThat(response.getStatusLine().getStatusCode(), equalTo(429));
        Map<?, ?> map = responseAsMap(response);
        assertMap(
            map,
            matchesMap().entry("status", 429)
                .entry(
                    "error",
                    matchesMap().extraOk()
                        .entry("bytes_wanted", greaterThan(1000))
                        .entry("reason", matchesRegex("\\[request] Data too large, data for \\[.+] would be .+"))
                        .entry("durability", "TRANSIENT")
                        .entry("type", "circuit_breaking_exception")
                        .entry("bytes_limit", greaterThan(1000))
                        .entry("root_cause", matchesList().item(any(Map.class)))
                )
        );
    }

    public void testSortByManyLongsGiantTopN() throws IOException {
        initManyLongs(10);
        assertMap(
            sortBySomeLongsLimit(100000),
            matchesMap().entry("took", greaterThan(0))
                .entry("is_partial", false)
                .entry("columns", List.of(Map.of("name", "MAX(a)", "type", "long")))
                .entry("values", List.of(List.of(9)))
                .entry("documents_found", greaterThan(0))
                .entry("values_loaded", greaterThan(0))
                .entry("completion_time_in_millis", greaterThan(0L))
                .entry("expiration_time_in_millis", greaterThan(0L))
                .entry("start_time_in_millis", greaterThan(0L))
        );
    }

    public void testSortByManyLongsGiantTopNTooMuchMemory() throws IOException {
        initManyLongs(20);
        assertCircuitBreaks(attempt -> sortBySomeLongsLimit(attempt * 500000));
    }

    public void testStupidTopN() throws IOException {
        initManyLongs(1); // Doesn't actually matter how much data there is.
        assertCircuitBreaks(attempt -> sortBySomeLongsLimit(2147483630));
    }

    private void assertFoldCircuitBreaks(TryCircuitBreaking tryBreaking) throws IOException {
        assertCircuitBreaks(
            tryBreaking,
            matchesMap().entry("status", 400).entry("error", matchesMap().extraOk().entry("type", "fold_too_much_memory_exception"))
        );
    }

    private void assertParseFailure(ThrowingRunnable r) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, r);
        Map<?, ?> map = responseAsMap(e.getResponse());
        logger.info("expected parse failure {}", map);
        assertMap(map, matchesMap().entry("status", 400).entry("error", matchesMap().extraOk().entry("type", "parsing_exception")));
    }

    private Map<String, Object> sortByManyLongs(int count) throws IOException {
        logger.info("sorting by {} longs", count);
        return responseAsMap(query(makeSortByManyLongs(count).toString(), null));
    }

    private StringBuilder makeSortByManyLongs(int count) {
        StringBuilder query = makeManyLongs(count);
        query.append("| SORT a, b, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| KEEP a, b | LIMIT 10000\"}");
        return query;
    }

    private Map<String, Object> sortBySomeLongsLimit(int count) throws IOException {
        logger.info("sorting by 5 longs, keeping {}", count);
        return responseAsMap(query(makeSortBySomeLongsLimit(count), null));
    }

    private String makeSortBySomeLongsLimit(int count) {
        StringBuilder query = new StringBuilder("{\"query\": \"FROM manylongs\n");
        query.append("| SORT a, b, c, d, e\n");
        query.append("| LIMIT ").append(count).append("\n");
        query.append("| STATS MAX(a)\n");
        query.append("\"}");
        return query.toString();
    }

    /**
     * This groups on about 200 columns which is a lot but has never caused us trouble.
     */
    public void testGroupOnSomeLongs() throws IOException {
        initManyLongs(10);
        Response resp = groupOnManyLongs(200);
        Map<String, Object> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertResultMap(map, columns, values);
    }

    /**
     * This groups on 5000 columns which used to throw a {@link StackOverflowError}.
     */
    public void testGroupOnManyLongs() throws IOException {
        initManyLongs(10);
        Response resp = groupOnManyLongs(5000);
        Map<String, Object> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(a)").entry("type", "long"));
        ListMatcher values = matchesList().item(List.of(9));
        assertResultMap(map, columns, values);
    }

    private Response groupOnManyLongs(int count) throws IOException {
        logger.info("grouping on {} longs", count);
        StringBuilder query = makeManyLongs(count);
        query.append("| STATS MIN(a) BY a, b, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        query.append("\\n| STATS MAX(a)\"}");
        return query(query.toString(), null);
    }

    private StringBuilder makeManyLongs(int count) {
        StringBuilder query = startQuery();
        query.append("FROM manylongs\\n| EVAL i0 = a + b, i1 = b + i0");
        for (int i = 2; i < count; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + ").append(i - 1);
        }
        return query.append("\\n");
    }

    public void testSmallConcat() throws IOException {
        initSingleDocIndex();
        Response resp = concat(2);
        Map<String, Object> map = responseAsMap(resp);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "a").entry("type", "long"))
            .item(matchesMap().entry("name", "str").entry("type", "keyword"));
        ListMatcher values = matchesList().item(List.of(1, "1".repeat(100)));
        assertResultMap(map, columns, values);
    }

    public void testHugeConcat() throws IOException {
        initSingleDocIndex();
        ResponseException e = expectThrows(ResponseException.class, () -> concat(10));
        Map<?, ?> map = responseAsMap(e.getResponse());
        logger.info("expected request rejected {}", map);
        MapMatcher mapMatcher = matchesMap();
        assertMap(
            map,
            mapMatcher.entry("status", 400)
                .entry("error", matchesMap().extraOk().entry("reason", "concatenating more than [1048576] bytes is not supported"))
        );
    }

    private Response concat(int evals) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM single | EVAL str = TO_STRING(a)");
        for (int e = 0; e < evals; e++) {
            query.append("\n| EVAL str=CONCAT(")
                .append(IntStream.range(0, 10).mapToObj(i -> "str").collect(Collectors.joining(", ")))
                .append(")");
        }
        query.append("\"}");
        return query(query.toString(), null);
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyConcat() throws IOException {
        int strings = 300;
        initManyLongs(10);
        assertManyStrings(manyConcat("FROM manylongs", strings), strings);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyConcat() throws IOException {
        initManyLongs(10);
        // 2000 is plenty to break on most nodes
        assertCircuitBreaks(attempt -> manyConcat("FROM manylongs", attempt * 2000));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyConcatFromRow() throws IOException {
        int strings = 2000;
        assertManyStrings(manyConcat("ROW a=9999, b=9999, c=9999, d=9999, e=9999", strings), strings);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyConcatFromRow() throws IOException {
        // 5000 is plenty to break on most nodes
        assertFoldCircuitBreaks(
            attempt -> manyConcat(
                "ROW a=9999999999999, b=99999999999999999, c=99999999999999999, d=99999999999999999, e=99999999999999999",
                attempt * 5000
            )
        );
    }

    /**
     * Fails to parse a huge huge query.
     */
    public void testHugeHugeManyConcatFromRow() throws IOException {
        assertParseFailure(() -> manyConcat("ROW a=9999, b=9999, c=9999, d=9999, e=9999", 6000));
    }

    /**
     * Tests that generate many moderately long strings.
     */
    private Map<String, Object> manyConcat(String init, int strings) throws IOException {
        StringBuilder query = startQuery();
        query.append(init).append(" | EVAL str = CONCAT(");
        query.append(
            Arrays.stream(new String[] { "a", "b", "c", "d", "e" })
                .map(f -> "TO_STRING(" + f + ")")
                .collect(Collectors.joining(", \\\"  \\\", "))
        );
        query.append(")\n| EVAL ");
        for (int s = 0; s < strings; s++) {
            if (s != 0) {
                query.append(",");
            }
            query.append("\nstr")
                .append(s)
                .append("=CONCAT(")
                .append(IntStream.range(0, 30).mapToObj(i -> "str").collect(Collectors.joining(", ")))
                .append(")");
        }
        query.append("\n|KEEP ");
        for (int s = 0; s < strings; s++) {
            if (s != 0) {
                query.append(", ");
            }
            query.append("str").append(s);
        }
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyRepeat() throws IOException {
        int strings = 30;
        initManyLongs(10);
        assertManyStrings(manyRepeat("FROM manylongs", strings), 30);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyRepeat() throws IOException {
        initManyLongs(10);
        // 75 is plenty to break on most nodes
        assertCircuitBreaks(attempt -> manyRepeat("FROM manylongs", attempt * 75));
    }

    /**
     * Returns many moderately long strings.
     */
    public void testManyRepeatFromRow() throws IOException {
        int strings = 300;
        assertManyStrings(manyRepeat("ROW a = 99", strings), strings);
    }

    /**
     * Hits a circuit breaker by building many moderately long strings.
     */
    public void testHugeManyRepeatFromRow() throws IOException {
        // 400 is enough to break on most nodes
        assertFoldCircuitBreaks(attempt -> manyRepeat("ROW a = 99", attempt * 400));
    }

    /**
     * Fails to parse a huge, huge query.
     */
    public void testHugeHugeManyRepeatFromRow() throws IOException {
        assertParseFailure(() -> manyRepeat("ROW a = 99", 100000));
    }

    /**
     * Tests that generate many moderately long strings.
     */
    private Map<String, Object> manyRepeat(String init, int strings) throws IOException {
        StringBuilder query = startQuery();
        query.append(init).append(" | EVAL str = TO_STRING(a)");
        for (int s = 0; s < strings; s++) {
            query.append(",\nstr").append(s).append("=REPEAT(str, 10000)");
        }
        query.append("\n|KEEP ");
        for (int s = 0; s < strings; s++) {
            if (s != 0) {
                query.append(", ");
            }
            query.append("str").append(s);
        }
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private void assertManyStrings(Map<String, Object> resp, int strings) throws IOException {
        ListMatcher columns = matchesList();
        for (int s = 0; s < strings; s++) {
            columns = columns.item(matchesMap().entry("name", "str" + s).entry("type", "keyword"));
        }
        MapMatcher mapMatcher = matchesMap();
        assertMap(resp, mapMatcher.entry("columns", columns));
    }

    public void testManyEval() throws IOException {
        initManyLongs(10);
        Map<String, Object> response = manyEval(1);
        ListMatcher columns = matchesList();
        columns = columns.item(matchesMap().entry("name", "a").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "b").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "c").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "d").entry("type", "long"));
        columns = columns.item(matchesMap().entry("name", "e").entry("type", "long"));
        for (int i = 0; i < 20; i++) {
            columns = columns.item(matchesMap().entry("name", "i0" + i).entry("type", "long"));
        }
        assertResultMap(response, columns, hasSize(10_000));
    }

    public void testTooManyEval() throws IOException {
        initManyLongs(10);
        // 490 is plenty to fail on most nodes
        assertCircuitBreaks(attempt -> manyEval(attempt * 490));
    }

    private Map<String, Object> manyEval(int evalLines) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manylongs");
        for (int e = 0; e < evalLines; e++) {
            query.append("\n| EVAL ");
            for (int i = 0; i < 20; i++) {
                if (i != 0) {
                    query.append(", ");
                }
                query.append("i").append(e).append(i).append(" = ").append(e * 10 + i).append(" + a + b");
            }
        }
        query.append("\n| LIMIT 10000\"}");
        return responseAsMap(query(query.toString(), null));
    }

    public void testFetchManyBigFields() throws IOException {
        initManyBigFieldsIndex(100, "keyword", false);
        Map<?, ?> response = fetchManyBigFields(100);
        ListMatcher columns = matchesList();
        for (int f = 0; f < 1000; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testFetchTooManyBigFields() throws IOException {
        initManyBigFieldsIndex(500, "keyword", false);
        // 500 docs is plenty to circuit break on most nodes
        assertCircuitBreaks(attempt -> fetchManyBigFields(attempt * 500));
    }

    /**
     * Fetches documents containing 1000 fields which are {@code 1kb} each.
     */
    private Map<String, Object> fetchManyBigFields(int docs) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | SORT f000 | LIMIT " + docs + "\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    public void testAggManyBigTextFields() throws IOException {
        int docs = 100;
        int fields = 100;
        initManyBigFieldsIndex(docs, "text", false);
        Map<?, ?> response = aggManyBigFields(fields);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        assertMap(
            response,
            matchesMap().entry("columns", columns).entry("values", matchesList().item(matchesList().item(1024 * fields * docs)))
        );
    }

    /**
     * Aggregates documents containing many fields which are {@code 1kb} each.
     */
    private Map<String, Object> aggManyBigFields(int fields) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM manybigfields | STATS sum = SUM(");
        query.append("LENGTH(f").append(String.format(Locale.ROOT, "%03d", 0)).append(")");
        for (int f = 1; f < fields; f++) {
            query.append(" + LENGTH(f").append(String.format(Locale.ROOT, "%03d", f)).append(")");
        }
        query.append(")\"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    /**
     * Aggregates on the {@code LENGTH} of a giant text field. Without
     * splitting pages on load (#131053) this throws a {@link CircuitBreakingException}
     * when it tries to load a giant field. With that change it finishes
     * after loading many single-row pages.
     */
    public void testAggGiantTextField() throws IOException {
        int docs = 100;
        initGiantTextField(docs, false, 5);
        Map<?, ?> response = aggGiantTextField();
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        assertMap(
            response,
            matchesMap().entry("columns", columns).entry("values", matchesList().item(matchesList().item(1024 * 1024 * 5 * docs)))
        );
    }

    /**
     * Aggregates documents containing a text field that is {@code 1mb} each.
     */
    private Map<String, Object> aggGiantTextField() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM bigtext | STATS sum = SUM(LENGTH(f))\"}");
        return responseAsMap(query(query.toString(), "columns,values"));
    }

    public void testAggMvLongs() throws IOException {
        int fieldValues = 100;
        initMvLongsIndex(1, 3, fieldValues, false);
        Map<?, ?> response = aggMvLongs(3);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(f00)").entry("type", "long"))
            .item(matchesMap().entry("name", "f00").entry("type", "long"))
            .item(matchesMap().entry("name", "f01").entry("type", "long"))
            .item(matchesMap().entry("name", "f02").entry("type", "long"));
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testAggTooManyMvLongs() throws IOException {
        initMvLongsIndex(1, 3, 1000, false);
        // 3 fields is plenty on most nodes
        assertCircuitBreaks(attempt -> aggMvLongs(attempt * 3));
    }

    private Map<String, Object> aggMvLongs(int fields) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs | STATS MAX(f00) BY f00");
        for (int f = 1; f < fields; f++) {
            query.append(", f").append(String.format(Locale.ROOT, "%02d", f));
        }
        return responseAsMap(query(query.append("\"}").toString(), "columns"));
    }

    public void testFetchMvLongs() throws IOException {
        int fields = 100;
        initMvLongsIndex(100, fields, 1000, false);
        Map<?, ?> response = fetchMvLongs();
        ListMatcher columns = matchesList();
        for (int f = 0; f < fields; f++) {
            columns = columns.item(matchesMap().entry("name", String.format(Locale.ROOT, "f%02d", f)).entry("type", "long"));
        }
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testFetchTooManyMvLongs() throws IOException {
        initMvLongsIndex(500, 100, 1000, false);
        assertCircuitBreaks(attempt -> fetchMvLongs());
    }

    private Map<String, Object> fetchMvLongs() throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM mv_longs\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    public void testManyExponentialHistograms() throws IOException {
        initManyExponentialHistograms(10_000, 100);

        // Run a successful query first as sanity check
        queryAndVerifyDuplicatedHistograms("many_exponential_histograms", "histo", 1);

        // and now blow up the memory
        assertCircuitBreaks(attempt -> queryDuplicatedHistograms("many_exponential_histograms", "histo", attempt * 10));
    }

    public void testManyTDigests() throws IOException {
        initManyTDigests(10_000, 100, TDigestFieldType.TDIGEST);

        // Run a successful query first as sanity check
        queryAndVerifyDuplicatedHistograms("many_tdigests", "histo", 1);

        // and now blow up the memory
        assertCircuitBreaks(attempt -> queryDuplicatedHistograms("many_tdigests", "histo", attempt * 10));
    }

    public void testManyHistograms() throws IOException {
        initManyTDigests(10_000, 100, TDigestFieldType.HISTOGRAM);

        // Run a successful query first as sanity check
        queryAndVerifyDuplicatedHistograms("many_tdigests", "TO_TDIGEST(histo)", 1);

        // and now blow up the memory
        assertCircuitBreaks(attempt -> queryDuplicatedHistograms("many_tdigests", "TO_TDIGEST(histo)", attempt * 10));
    }

    private void queryAndVerifyDuplicatedHistograms(String index, String column, int numDuplications) throws IOException {
        Map<String, Object> responseMap = queryDuplicatedHistograms(index, column, numDuplications);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "dummy").entry("type", "double"));
        ListMatcher values = matchesList(List.of(matchesList(List.of(isA(Double.class)))));
        assertResultMap(responseMap, columns, values);
    }

    /**
     * Creates a query which loads each histogram n-times into memory.
     * We do this by querying n percentiles on each histogram, each with a different filter condition which however never is false.
     * PERCENTILES() is implemented using a surrogate histogram merge, which means at the end of the STATS we
     * have n copies of each histogram in memory.
     */
    private Map<String, Object> queryDuplicatedHistograms(String index, String column, int numDuplications) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM " + index);
        query.append("| STATS ");
        query.append(
            IntStream.range(0, numDuplications)
                .mapToObj(i -> "val_" + i + " = PERCENTILE(" + column + ", 50) WHERE histo_id != -" + i)
                .collect(Collectors.joining(", "))
        );
        query.append("BY histo_id");
        // in the end aggregate it all to a single sum to not blow up the result set
        query.append("| EVAL vals_sum = ");
        query.append(IntStream.range(0, numDuplications).mapToObj(i -> "val_" + i).collect(Collectors.joining(" + ")));
        query.append("| STATS dummy = SUM(vals_sum)\"}");
        String queryStr = query.toString().replace("\n", "\\n");
        return responseAsMap(query(queryStr, null));
    }

    private void initManyLongs(int countPerLong) throws IOException {
        logger.info("loading many documents with longs");
        StringBuilder bulk = new StringBuilder();
        int flush = 0;
        for (int a = 0; a < countPerLong; a++) {
            for (int b = 0; b < countPerLong; b++) {
                for (int c = 0; c < countPerLong; c++) {
                    for (int d = 0; d < countPerLong; d++) {
                        for (int e = 0; e < countPerLong; e++) {
                            bulk.append(String.format(Locale.ROOT, """
                                {"create":{}}
                                {"a":%d,"b":%d,"c":%d,"d":%d,"e":%d}
                                """, a, b, c, d, e));
                            flush++;
                            if (flush % 10_000 == 0) {
                                bulk("manylongs", bulk.toString());
                                bulk.setLength(0);
                                logger.info(
                                    "flushing {}/{} to manylongs",
                                    flush,
                                    countPerLong * countPerLong * countPerLong * countPerLong * countPerLong
                                );

                            }
                        }
                    }
                }
            }
        }
        initIndex("manylongs", bulk.toString());
    }

    private void initSingleDocIndex() throws IOException {
        logger.info("loading a single document");
        initIndex("single", """
            {"create":{}}
            {"a":1}
            """);
    }

    void initManyBigFieldsIndex(int docs, String type, boolean random) throws IOException {
        logger.info("loading many documents with many big fields");
        int docsPerBulk = 5;
        int fields = 1000;
        int fieldSize = Math.toIntExact(ByteSizeValue.ofKb(1).getBytes());

        Request request = new Request("PUT", "/manybigfields");
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        config.startObject("settings").field("index.mapping.total_fields.limit", 10000).endObject();
        config.startObject("mappings").startObject("properties");
        for (int f = 0; f < fields; f++) {
            config.startObject("f" + String.format(Locale.ROOT, "%03d", f)).field("type", type).endObject();
        }
        config.endObject().endObject();
        request.setJsonEntity(Strings.toString(config.endObject()));
        Response response = client().performRequest(request);
        assertThat(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            equalTo("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"manybigfields\"}")
        );

        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            for (int f = 0; f < fields; f++) {
                if (f == 0) {
                    bulk.append('{');
                } else {
                    bulk.append(", ");
                }
                bulk.append('"').append("f").append(String.format(Locale.ROOT, "%03d", f)).append("\": \"");
                // if requested, generate random string to hit the CBE faster
                bulk.append(random ? randomAlphaOfLength(1024) : Integer.toString(f % 10).repeat(fieldSize));
                bulk.append('"');
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk("manybigfields", bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex("manybigfields", bulk.toString());
    }

    void initGiantTextField(int docs, boolean includeId, long fieldSizeInMb) throws IOException {
        int docsPerBulk = isServerless() ? 3 : 10;
        logger.info("loading many documents with one big text field - docs per bulk {}", docsPerBulk);

        int fieldSize = Math.toIntExact(ByteSizeValue.ofMb(fieldSizeInMb).getBytes());

        Request request = new Request("PUT", "/bigtext");
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        config.startObject("mappings").startObject("properties");
        if (includeId) {
            config.startObject("id").field("type", "long").endObject();
        }
        config.startObject("f").field("type", "text").endObject();
        config.endObject().endObject();
        request.setJsonEntity(Strings.toString(config.endObject()));
        Response response = client().performRequest(request);
        assertThat(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            equalTo("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"bigtext\"}")
        );

        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            if (includeId) {
                String s = String.format(Locale.ROOT, "{\"id\":\"%s\", \"f\":\"", d);
                bulk.append(s);
            } else {
                bulk.append("{\"f\":\"");
            }
            bulk.append(Integer.toString(d % 10).repeat(fieldSize));
            bulk.append("\"}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk("bigtext", bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex("bigtext", bulk.toString());
        logger.info("loaded");
    }

    /**
     * Tests that FIRST agg with a large grouping state trips the circuit breaker.
     * We don't require many fields in the index. However, for the small number of fields that we have, we want them to have a large
     * number of multivalues. We also require a large number of documents. Further, our query groups by a unique id field, in order to
     * generate a large of buckets. All the aforementioned participates in inflating the size of the grouping state.
     */
    public void testFirstAggWithManyLongs() throws IOException {
        initMvLongsIndex(5000, 2, 3000, true);
        assertCircuitBreaks(attempt -> aggregateByIdOnManyLongs("FIRST"));
    }

    /**
     * Tests that LAST agg with a large grouping state trips the circuit breaker.
     * @throws IOException
     * @see #testFirstAggWithManyLongs()
     */
    public void testLastAggWithManyLongs() throws IOException {
        initMvLongsIndex(5000, 2, 3000, true);
        assertCircuitBreaks(attempt -> aggregateByIdOnManyLongs("LAST"));
    }

    /**
     * Tests that FIRST agg with huge text fields trips the circuit breaker.
     * @throws IOException
     * @see #testFirstAggWithManyLongs()
     */
    public void testFirstAggWithGiantText() throws IOException {
        initGiantTextField(50, true, 3);
        assertCircuitBreaks(attempt -> aggregateByIdOnLargeText("FIRST"));
    }

    /**
     * Tests that LAST agg with huge text fields trips the circuit breaker.
     * @throws IOException
     * @see #testFirstAggWithManyLongs()
     */
    public void testLastAggWithGiantText() throws IOException {
        initGiantTextField(50, true, 3);
        assertCircuitBreaks(attempt -> aggregateByIdOnLargeText("LAST"));
    }

    private Map<String, Object> aggregateByIdOnLargeText(String aggregation) throws IOException {
        StringBuilder query = new StringBuilder("{\"query\": \"FROM bigtext\n");
        // Grouping on the unique id field generates a large number of buckets where each bucket's value contains the
        // entire set of multivalues.
        String aggClause = "| STATS x = %s(f, id) BY id\n";
        query.append(String.format(Locale.ROOT, aggClause, aggregation));
        query.append("\"}");
        return responseAsMap(query(query.toString(), "columns"));
    }

    private Map<String, Object> aggregateByIdOnManyLongs(String aggregation) throws IOException {
        StringBuilder query = new StringBuilder("{\"query\": \"FROM mv_longs\n");
        // Grouping on the unique id field generates a large number of buckets where each bucket's value contains the
        // entire set of multivalues.
        query.append(String.format(Locale.ROOT, "| STATS x = %s(f00, f01) BY id\n", aggregation));
        query.append("\"}");

        return responseAsMap(query(query.toString(), "columns"));
    }

    private void initMvLongsIndex(int docs, int fields, int fieldValues, boolean includeId) throws IOException {
        logger.info("loading documents with many multivalued longs");
        int docsPerBulk = 100;

        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            for (int f = 0; f < fields; f++) {
                if (f == 0) {
                    bulk.append('{');
                    if (includeId) {
                        String idField = String.format("\"id\": %s, ", d);
                        bulk.append(idField);
                    }
                } else {
                    bulk.append(", ");
                }
                bulk.append('"').append("f").append(String.format(Locale.ROOT, "%02d", f)).append("\": ");
                for (int fv = 0; fv < fieldValues; fv++) {
                    if (fv == 0) {
                        bulk.append('[');
                    } else {
                        bulk.append(", ");
                    }
                    bulk.append(f + fv);
                }
                bulk.append(']');
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk("mv_longs", bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex("mv_longs", bulk.toString());
    }

    private void initManyExponentialHistograms(int numHistograms, int numBucketsPerHistogram) throws IOException {
        logger.info("loading many documents with exponential histograms");

        createIndex("many_exponential_histograms", Settings.EMPTY, """
            {
                "properties": {
                  "histo": {
                    "type": "exponential_histogram"
                  },
                  "histo_id": {
                    "type": "long"
                  }
                }
            }
            """);

        StringBuilder bulk = new StringBuilder();
        int flush = 0;
        for (int i = 0; i < numHistograms; i++) {

            // The scale doesn't actually matter here
            ExponentialHistogramBuilder builder = ExponentialHistogram.builder(10, ExponentialHistogramCircuitBreaker.noop());
            for (int j = 0; j < numBucketsPerHistogram; j++) {
                builder.setPositiveBucket(i + j, 1 + i + j * 2);
            }
            String histoJson;
            try (XContentBuilder xContentBuilder = JsonXContent.contentBuilder()) {
                ExponentialHistogramXContent.serialize(xContentBuilder, builder.build());
                histoJson = Strings.toString(xContentBuilder);
            }
            ;

            bulk.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"histo_id":%d,"histo":%s}
                """, i + 1, histoJson));
            flush++;
            if (flush % 10_000 == 0) {
                bulk("many_exponential_histograms", bulk.toString());
                bulk.setLength(0);
                logger.info("flushing {}/{} to many_exponential_histograms", flush, numHistograms);
            }
        }
        // Load the remaining data and also do a force merge
        initIndex("many_exponential_histograms", bulk.toString());
    }

    enum TDigestFieldType {
        TDIGEST,
        HISTOGRAM
    }

    private void initManyTDigests(int numHistograms, int numCentroidsPerHistogram, TDigestFieldType fieldType) throws IOException {
        logger.info("loading many documents with tdigests");

        createIndex("many_tdigests", Settings.EMPTY, """
            {
                "properties": {
                  "histo": {
                    "type": "%s"
                  },
                  "histo_id": {
                    "type": "long"
                  }
                }
            }
            """.formatted(fieldType == TDigestFieldType.TDIGEST ? "tdigest" : "histogram"));

        StringBuilder bulk = new StringBuilder();
        int flush = 0;
        String centroidsFieldName = fieldType == TDigestFieldType.TDIGEST ? "centroids" : "values";
        for (int i = 0; i < numHistograms; i++) {
            StringBuilder histoJson = new StringBuilder("{");
            histoJson.append("\"").append(centroidsFieldName).append("\":");
            histoJson.append(
                IntStream.range(i, i + numCentroidsPerHistogram).mapToObj(Integer::toString).collect(Collectors.joining(",", "[", "]"))
            );
            histoJson.append(",\"counts\":");
            int finalI = i;
            histoJson.append(
                IntStream.range(0, numCentroidsPerHistogram)
                    .map(j -> 1 + finalI + (j * 2))
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining(",", "[", "]"))
            );
            histoJson.append("}");

            bulk.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"histo_id":%d,"histo":%s}
                """, i + 1, histoJson));
            flush++;
            if (flush % 10_000 == 0) {
                bulk("many_tdigests", bulk.toString());
                bulk.setLength(0);
                logger.info("flushing {}/{} to many_tdigests", flush, numHistograms);
            }
        }
        // Load the remaining data and also do a force merge
        initIndex("many_tdigests", bulk.toString());
    }

}
