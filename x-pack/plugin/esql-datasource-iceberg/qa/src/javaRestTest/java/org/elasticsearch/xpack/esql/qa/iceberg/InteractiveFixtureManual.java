/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.iceberg;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.S3RequestLog;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.PrintStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Booleans.parseBoolean;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;

/**
 * Interactive fixture runner for manual testing of ESQL External command with Parquet/S3.
 * <p>
 * <b>IMPORTANT:</b> This class is named "Manual" (not "IT" or "Test") to prevent automatic
 * execution during regular builds. It must be explicitly selected to run.
 * <p>
 * This starts:
 * <ul>
 *   <li>S3HttpFixture on port 9345 serving Parquet files from src/test/resources/iceberg-fixtures/</li>
 *   <li>Elasticsearch cluster on port 9200 configured to access the fixture via S3</li>
 * </ul>
 * <p>
 * Then waits indefinitely (or for configured time) to allow manual queries via curl,
 * Kibana Dev Console, or other tools.
 * <p>
 * <b>Usage:</b>
 * <pre>
 * # Explicit test selection (required):
 * ./gradlew :x-pack:plugin:esql:qa:server:iceberg:javaRestTest \
 *   --tests "*InteractiveFixtureManual*"
 * </pre>
 * <p>
 * <b>Optional System Properties:</b>
 * <ul>
 *   <li>{@code -Dtests.fixture.wait_minutes=N} - Wait N minutes (0 = indefinite, default: 0)</li>
 *   <li>{@code -Dtests.fixture.show_blobs=true} - List all loaded fixtures (default: false)</li>
 *   <li>{@code -Dtests.fixture.show_logs=false} - Show S3 request logs (default: true)</li>
 * </ul>
 * <p>
 * <b>Fixed Ports:</b>
 * <ul>
 *   <li>Elasticsearch: http://localhost:9200</li>
 *   <li>S3/HTTP Fixture: http://localhost:9345</li>
 * </ul>
 * Press Ctrl+C to stop when running indefinitely.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
@TimeoutSuite(millis = 7 * 24 * 60 * 60 * 1000) // 7 days - effectively no timeout
@AwaitsFix(bugUrl = "Iceberg integration tests disabled pending stabilization")
public class InteractiveFixtureManual extends ESRestTestCase {

    /** Fixed port for Elasticsearch */
    private static final int ES_PORT = 9200;

    /** Fixed port for S3/HTTP fixture */
    private static final int S3_FIXTURE_PORT = 9345;

    private static final PrintStream out = stderr();

    /** S3 HTTP fixture serving test data on fixed port */
    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture(S3_FIXTURE_PORT);

    /** Elasticsearch cluster with S3 fixture for interactive testing on fixed port */
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        // Fixed port for easy access
        .setting("http.port", String.valueOf(ES_PORT))
        // Enable S3 repository plugin for S3 access
        .module("repository-s3")
        // Basic cluster settings
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        // Disable ML to avoid native code loading issues in some environments
        .setting("xpack.ml.enabled", "false")
        // S3 client configuration for accessing the S3HttpFixture
        .setting("s3.client.default.endpoint", () -> s3Fixture.getAddress())
        // S3 credentials must be stored in keystore, not as regular settings
        .keystore("s3.client.default.access_key", ACCESS_KEY)
        .keystore("s3.client.default.secret_key", SECRET_KEY)
        // Disable SSL for HTTP fixture
        .setting("s3.client.default.protocol", "http")
        // Disable AWS SDK profile file loading
        .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
        .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
        // Arrow's unsafe memory allocator requires access to java.nio internals
        .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .jvmArg("-Darrow.allocation.manager.type=Unsafe")
        .build();

    /** Rule chain ensures s3Fixture starts before cluster (cluster depends on s3Fixture address) */
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    // Wait time in minutes (configurable via system property, 0 = indefinite)
    private static final int WAIT_MINUTES = Integer.parseInt(System.getProperty("tests.fixture.wait_minutes", "0"));

    // Whether to show all loaded fixtures
    private static final boolean SHOW_BLOBS = parseBoolean(System.getProperty("tests.fixture.show_blobs", "false"));

    // Whether to show S3 request logs during interactive session
    private static final boolean SHOW_LOGS = parseBoolean(System.getProperty("tests.fixture.show_logs", "true"));

    // Message templates for output
    private MessageTemplates messages;

    @BeforeClass
    public static void loadFixtures() {
        s3Fixture.loadFixturesFromResources();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Main interactive entry point that starts the fixture and cluster, then waits.
     * This is a "test" only in name - it doesn't assert anything, just keeps the fixture running.
     */
    public void testInteractiveMode() throws Exception {
        // Load message templates
        loadMessages();

        // Display information
        messages.print("banner");
        printClusterInfo();
        printFixtureInfo();
        printAvailableFixtures();
        messages.print("example_queries");
        printWaitMessage();

        // Wait for the specified duration
        waitWithProgress(WAIT_MINUTES);

        if (SHOW_LOGS) {
            printRequestLogs();
        }

        messages.print("shutdown");
    }

    private void loadMessages() throws Exception {
        messages = MessageTemplates.load("/interactive-fixture-messages.txt");

        // Set common variables
        String fixtureUrl = s3Fixture.getAddress();
        messages.set("es_url", cluster.getHttpAddresses())
            .set("s3_endpoint", fixtureUrl)
            .set("fixture_url", fixtureUrl)
            .set("bucket", BUCKET)
            .set("warehouse", WAREHOUSE)
            .set("access_key", ACCESS_KEY)
            .set("secret_key", SECRET_KEY);

        // Extract port from URL
        try {
            java.net.URI uri = new java.net.URI(fixtureUrl);
            int port = uri.getPort();
            messages.set("port", port > 0 ? String.valueOf(port) : "default");
        } catch (Exception e) {
            messages.set("port", "(unable to parse)");
        }
    }

    private void printClusterInfo() {
        messages.print("cluster_info");
    }

    private void printFixtureInfo() {
        messages.print("fixture_info");
    }

    private void printAvailableFixtures() {
        var handler = s3Fixture.getHandler();
        var blobs = handler.blobs();

        // Count fixtures by type
        long parquetCount = blobs.keySet().stream().filter(key -> key.endsWith(".parquet")).count();
        long metadataCount = blobs.keySet().stream().filter(key -> key.contains("metadata")).count();
        long otherCount = blobs.size() - parquetCount - metadataCount;

        messages.set("total_files", blobs.size())
            .set("parquet_count", parquetCount)
            .set("metadata_count", metadataCount)
            .set("other_count", otherCount > 0 ? String.valueOf(otherCount) : "");

        messages.print("fixtures_header");

        if (SHOW_BLOBS) {
            messages.print("fixtures_show_all");
            blobs.keySet().stream().sorted().forEach(key -> {
                long size = blobs.get(key).length();
                out.printf(Locale.ROOT, "  %-80s %10s%n", key, MessageTemplates.formatBytes(size));
            });
        } else {
            messages.print("fixtures_show_key");
            blobs.keySet().stream().filter(key -> key.contains("employees") || key.contains("standalone")).sorted().forEach(key -> {
                long size = blobs.get(key).length();
                out.printf(Locale.ROOT, "  %-80s %10s%n", key, MessageTemplates.formatBytes(size));
            });
            messages.print("fixtures_footer");
        }
    }

    private void printWaitMessage() {
        if (WAIT_MINUTES == 0) {
            messages.print("wait_indefinite");
        } else {
            messages.set("wait_minutes", WAIT_MINUTES);
            messages.print("wait_timed");
        }
    }

    private void waitWithProgress(int minutes) throws InterruptedException {
        long intervalMillis = 60L * 1000L; // Update every minute

        if (minutes == 0) {
            // Run indefinitely
            long startTime = System.currentTimeMillis();
            while (true) {
                Thread.sleep(intervalMillis);
                long elapsedMillis = System.currentTimeMillis() - startTime;
                long elapsedMinutes = elapsedMillis / (60L * 1000L);
                long elapsedSeconds = (elapsedMillis % (60L * 1000L)) / 1000L;

                messages.set("elapsed_time", MessageTemplates.formatTime(elapsedMinutes, elapsedSeconds));
                messages.print("progress_indefinite");
            }
        } else {
            // Run for specified time
            long totalMillis = minutes * 60L * 1000L;
            long elapsedMillis = 0;
            long startTime = System.currentTimeMillis();

            while (elapsedMillis < totalMillis) {
                Thread.sleep(intervalMillis);
                elapsedMillis = System.currentTimeMillis() - startTime;

                long remainingMillis = totalMillis - elapsedMillis;
                long remainingMinutes = remainingMillis / (60L * 1000L);
                long remainingSeconds = (remainingMillis % (60L * 1000L)) / 1000L;

                messages.set("remaining_time", MessageTemplates.formatTime(remainingMinutes, remainingSeconds));
                messages.print("progress_timed");
            }
        }
    }

    private void printRequestLogs() {
        out.println();
        out.println("--------------------------------------------------------------------------------");
        out.println("S3 REQUEST LOG SUMMARY");
        out.println("--------------------------------------------------------------------------------");

        List<S3RequestLog> logs = S3FixtureUtils.getRequestLogs();

        if (logs.isEmpty()) {
            out.println("  No S3 requests were made during this session.");
            return;
        }

        out.println("  Total requests: " + logs.size());
        out.println();
        out.println("  Requests by type:");

        Map<String, Long> byType = logs.stream().collect(Collectors.groupingBy(S3RequestLog::getRequestType, Collectors.counting()));

        byType.entrySet()
            .stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .forEach(entry -> out.printf(Locale.ROOT, "    %-25s %5d%n", entry.getKey(), entry.getValue()));

        out.println();
        out.println("  Unique paths accessed:");
        logs.stream().map(S3RequestLog::getPath).distinct().sorted().limit(20).forEach(path -> out.printf(Locale.ROOT, "    %s%n", path));

        if (logs.stream().map(S3RequestLog::getPath).distinct().count() > 20) {
            out.println("    ... (showing first 20 paths)");
        }
    }

    @SuppressForbidden(reason = "System.err is intentional for this interactive manual testing tool")
    private static PrintStream stderr() {
        return System.err;
    }
}
