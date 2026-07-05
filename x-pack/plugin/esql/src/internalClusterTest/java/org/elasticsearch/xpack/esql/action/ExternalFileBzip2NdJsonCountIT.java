/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests: {@code FROM <dataset>} over HTTPS bzip2-compressed NDJSON (rally tracks), asserting
 * {@code STATS c = COUNT(*)}. The NYC taxis track is large; the suite timeout allows long scans.
 */
@TimeoutSuite(millis = 3 * TimeUnits.HOUR)
@ThreadLeakFilters(filters = ExternalFileBzip2NdJsonCountIT.JdkHttpClientThreadLeakFilter.class)
public class ExternalFileBzip2NdJsonCountIT extends AbstractExternalDataSourceIT {

    private static final String REMOTE_LOOKUP_JOIN_BZIP2_NDJSON = "https://rally-tracks.elastic.co/joins/lookup_idx_100000_f10.json.bz2";

    private static final long EXPECTED_LOOKUP_JOIN_COUNT = 100_000L;

    private static final String REMOTE_NYC_TAXIS_BZIP2_NDJSON = "https://rally-tracks.elastic.co/nyc_taxis/documents.json.bz2";

    private static final long EXPECTED_NYC_TAXIS_COUNT = 165_346_692L;

    /**
     * {@link java.net.http.HttpClient} may leave a {@code HttpClient-*-SelectorManager} thread running briefly
     * after external reads; internal-cluster node shutdown does not always close the client before suite
     * teardown runs thread-leak checks.
     */
    public static final class JdkHttpClientThreadLeakFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            String name = t.getName();
            return name.startsWith("HttpClient-") && name.endsWith("SelectorManager");
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(Bzip2DataSourcePlugin.class, NdJsonDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testExternalBzip2NdJsonStatsCount() {
        assertExternalBzip2NdJsonCount(REMOTE_LOOKUP_JOIN_BZIP2_NDJSON, EXPECTED_LOOKUP_JOIN_COUNT, TimeValue.timeValueMinutes(10));
    }

    @AwaitsFix(bugUrl = "Very long-running test")
    public void testExternalBzip2NdJsonStatsCountNycTaxisDocuments() {
        assertExternalBzip2NdJsonCount(REMOTE_NYC_TAXIS_BZIP2_NDJSON, EXPECTED_NYC_TAXIS_COUNT, TimeValue.timeValueHours(3));
    }

    private void assertExternalBzip2NdJsonCount(String remoteUrl, long expectedCount, TimeValue requestTimeout) {
        // bzip2 is outside the GA text-format codec set (uncompressed/gzip/zstd) and is rejected on release
        // builds; this end-to-end bzip2 read is therefore snapshot-only. See elastic/esql-planning#938.
        assumeTrue("bzip2 text-format codec is rejected on release builds", Build.current().isSnapshot());
        // The fixture lives on a remote host; if it is unreachable (offline CI, network policy) skip rather
        // than fail — an unavailable external resource is an environmental condition, not a code regression.
        assumeRemoteAvailable(remoteUrl);

        String dataset = registerDataset("bzip2_ndjson", remoteUrl, Map.of());
        String query = "FROM " + dataset + " | STATS c = COUNT(*)";

        try (var response = run(syncEsqlQueryRequest(query), requestTimeout)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.size(), equalTo(1));
            assertThat(columns.get(0).name(), equalTo("c"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            Object cell = rows.get(0).get(0);
            assertThat(((Number) cell).longValue(), equalTo(expectedCount));
        }
    }

    /**
     * Pre-flight reachability probe: skip (rather than fail) the test when the remote fixture is
     * missing or unreachable. A cheap {@code HEAD} is attempted first, falling back to a single-byte
     * ranged {@code GET} when the origin rejects {@code HEAD}; any I/O error or a non-2xx/3xx status
     * downgrades to a JUnit assumption violation. Reuses the JDK {@link HttpClient} so the suite's
     * {@link JdkHttpClientThreadLeakFilter} still covers any lingering selector thread.
     */
    private static void assumeRemoteAvailable(String url) {
        // try-with-resources: HttpClient is AutoCloseable (JDK 21+); closing it tears down the client's own
        // selector/worker threads so the probe does not add leaks that the suite's thread-leak check would flag.
        try (
            HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build()
        ) {
            HttpRequest head = HttpRequest.newBuilder(URI.create(url))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build();
            int status = client.send(head, HttpResponse.BodyHandlers.discarding()).statusCode();
            if (status == 405 || status == 501) { // HEAD unsupported -> probe with a 1-byte ranged GET
                HttpRequest ranged = HttpRequest.newBuilder(URI.create(url))
                    .header("Range", "bytes=0-0")
                    .GET()
                    .timeout(Duration.ofSeconds(10))
                    .build();
                status = client.send(ranged, HttpResponse.BodyHandlers.discarding()).statusCode();
            }
            assumeTrue("remote resource unavailable [" + url + "] (HTTP " + status + ")", status >= 200 && status < 400);
        } catch (IOException | InterruptedException e) {
            assumeNoException("remote resource unreachable [" + url + "]", e);
        }
    }
}
