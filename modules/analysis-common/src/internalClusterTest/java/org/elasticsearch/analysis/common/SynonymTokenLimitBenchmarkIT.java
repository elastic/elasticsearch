/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * DO NOT MERGE THIS TEST INTO MAIN. This is for manual testing only.
 * <p>
 * Exploratory benchmarks to find OOM and timeout thresholds for synonym analyzer builds.
 * Not intended for CI — run manually with {@code -Dtests.nightly=true}.
 *
 * <p>Each test writes progressively larger synonym files using a specific token generation
 * strategy and reloads the analyzer via the cluster, measuring end-to-end reload time
 * and heap usage including cluster overhead.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SynonymTokenLimitBenchmarkIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "synonym_benchmark_index";
    private static final String SYNONYMS_FILE = "benchmark_synonyms.txt";

    private static final int[] TOKEN_TIERS = {
        100_000,
        200_000,
        500_000,
        1_000_000,
        1_100_000,
        1_200_000,
        1_300_000,
        1_400_000,
        1_500_000,
        1_600_000,
        1_700_000,
        1_800_000,
        1_900_000,
        2_000_000 };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CommonAnalysisPlugin.class);
    }

    public void testClusterReloadRandomShort() throws Exception {
        runClusterReloadBenchmark("random short (3-6 chars)", 3, 6);
    }

    public void testClusterReloadRandomLong() throws Exception {
        runClusterReloadBenchmark("random long (10-25 chars)", 10, 25);
    }

    public void testClusterReloadRandomMixed() throws Exception {
        runClusterReloadBenchmark("random mixed (3-25 chars)", 3, 25);
    }

    public void testClusterReloadRandomExtraLong() throws Exception {
        runClusterReloadBenchmark("random extra-long (100-255 chars)", 100, 255);
    }

    private void runClusterReloadBenchmark(String label, int minTokenLen, int maxTokenLen) throws Exception {
        Path config = internalCluster().getInstance(Environment.class).configDir();
        Path synonymsFile = config.resolve(SYNONYMS_FILE);

        writeSynonymFile(synonymsFile, 1, minTokenLen, maxTokenLen);

        assertAcked(
            indicesAdmin().prepareCreate(INDEX_NAME)
                .setSettings(
                    indexSettings(1, 0).put("analysis.analyzer.bench_analyzer.tokenizer", "standard")
                        .put("analysis.analyzer.bench_analyzer.filter", "bench_synonym_filter")
                        .put("analysis.filter.bench_synonym_filter.type", "synonym_graph")
                        .put("analysis.filter.bench_synonym_filter.synonyms_path", SYNONYMS_FILE)
                        .put("analysis.filter.bench_synonym_filter.updateable", "true")
                        .put("analysis.filter.bench_synonym_filter.lenient", "true")
                )
                .setMapping("field", "type=text,analyzer=standard,search_analyzer=bench_analyzer")
        );
        ensureGreen(INDEX_NAME);

        suppressNoisyLoggers();

        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        logger.info("=== Cluster Reload Benchmark: {} ===", label);
        logger.info("Tier | Tokens   | Write (ms) | Reload (ms) | Tokens/ms | Heap Used | Heap Delta | Status");
        logger.info("-----|----------|------------|-------------|-----------|-----------|------------|-------");

        for (int tier = 0; tier < TOKEN_TIERS.length; tier++) {
            int tokenCount = TOKEN_TIERS[tier];

            long writeStart = System.nanoTime();
            writeSynonymFile(synonymsFile, tokenCount, minTokenLen, maxTokenLen);
            long writeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writeStart);

            System.gc();
            long heapBefore = memoryBean.getHeapMemoryUsage().getUsed();

            long reloadStart = System.nanoTime();
            String status;
            try {
                ReloadAnalyzersResponse reloadResponse = client().execute(
                    TransportReloadAnalyzersAction.TYPE,
                    new ReloadAnalyzersRequest(null, false, INDEX_NAME)
                ).actionGet(TimeValue.timeValueSeconds(120));

                long reloadMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reloadStart);

                System.gc();
                long heapAfter = memoryBean.getHeapMemoryUsage().getUsed();
                long heapDeltaMb = Math.max(0, (heapAfter - heapBefore)) / (1024 * 1024);
                long heapUsedMb = heapAfter / (1024 * 1024);

                if (reloadResponse.getFailedShards() > 0) {
                    Throwable cause = reloadResponse.getShardFailures()[0].getCause();
                    status = "SHARD_FAIL: " + rootCauseMessage(cause);
                } else {
                    status = reloadMs > 30_000 ? "SLOW (>30s)" : "OK";
                }

                long tokensPerMs = reloadMs > 0 ? tokenCount / reloadMs : 0;
                logger.info(
                    "{} | {} | {} | {} | {} | {} MB | {} MB | {}",
                    tier,
                    tokenCount,
                    writeMs,
                    reloadMs,
                    tokensPerMs,
                    heapUsedMb,
                    heapDeltaMb,
                    status
                );

                if (reloadMs > 30_000) {
                    logger.info("Reload exceeded 30 seconds at {} tokens — stopping", tokenCount);
                    break;
                }
                if (reloadResponse.getFailedShards() > 0) {
                    logger.info("Shard failures at {} tokens — stopping", tokenCount);
                    break;
                }
            } catch (Exception e) {
                long reloadMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reloadStart);
                long heapUsedMb = memoryBean.getHeapMemoryUsage().getUsed() / (1024 * 1024);
                status = rootCauseMessage(e);
                long tokensPerMs = reloadMs > 0 ? tokenCount / reloadMs : 0;
                logger.info(
                    "{} | {} | {} | {} | {} | {} MB | ? | {}",
                    tier,
                    tokenCount,
                    writeMs,
                    reloadMs,
                    tokensPerMs,
                    heapUsedMb,
                    status
                );
                logger.info("Exception at {} tokens — stopping", tokenCount);
                break;
            }
        }

        logger.info("=== Cluster Reload Benchmark Complete ===");
    }

    // ---- Helpers ----

    private static void writeSynonymFile(Path path, int targetTokens, int minTokenLen, int maxTokenLen) throws IOException {
        Random rng = new Random(42);
        int lines = Math.max(1, targetTokens / 3);
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            for (int i = 0; i < lines; i++) {
                writer.write(randomToken(rng, minTokenLen, maxTokenLen));
                writer.write(", ");
                writer.write(randomToken(rng, minTokenLen, maxTokenLen));
                writer.write(", ");
                writer.write(randomToken(rng, minTokenLen, maxTokenLen));
                writer.newLine();
            }
        }
    }

    private static String randomToken(Random rng, int minLen, int maxLen) {
        int len = minLen + rng.nextInt(maxLen - minLen + 1);
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) ('a' + rng.nextInt(26));
        }
        return new String(chars);
    }

    private static void suppressNoisyLoggers() {
        setLogLevel("org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction", Level.WARN);
        setLogLevel("org.elasticsearch.monitor.jvm.JvmGcMonitorService", Level.WARN);
    }

    private static void setLogLevel(String loggerName, Level level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);
        if (loggerConfig.getName().equals(loggerName) == false) {
            loggerConfig = new LoggerConfig(loggerName, level, true);
            config.addLogger(loggerName, loggerConfig);
        } else {
            loggerConfig.setLevel(level);
        }
        ctx.updateLoggers();
    }

    private static String rootCauseMessage(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
