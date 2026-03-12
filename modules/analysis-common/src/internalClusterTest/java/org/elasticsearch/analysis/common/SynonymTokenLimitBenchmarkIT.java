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
import org.apache.lucene.tests.util.LuceneTestCase;
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
 * strategy and reloads the analyzer via the cluster, incrementing by {@link #STEP} tokens
 * until failure or {@link #MAX_TOKENS} is reached.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@LuceneTestCase.Nightly
public class SynonymTokenLimitBenchmarkIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "synonym_benchmark_index";
    private static final String SYNONYMS_FILE = "benchmark_synonyms.txt";

    private static final int START_TOKENS = 100_000;
    private static final int STEP = 100_000;
    private static final int MAX_TOKENS = 10_000_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CommonAnalysisPlugin.class);
    }

    // ---- Random token tests ----

    public void testClusterReloadRandomShort() throws Exception {
        runBenchmarkWithThresholdSearch("random short (3-6 chars)", (rng, i) -> randomToken(rng, 3, 6));
    }

    public void testClusterReloadRandomLong() throws Exception {
        runBenchmarkWithThresholdSearch("random long (10-25 chars)", (rng, i) -> randomToken(rng, 10, 25));
    }

    public void testClusterReloadRandomMixed() throws Exception {
        runBenchmarkWithThresholdSearch("random mixed (3-25 chars)", (rng, i) -> randomToken(rng, 3, 25));
    }

    public void testClusterReloadRandomExtraLong() throws Exception {
        runBenchmarkWithThresholdSearch("random extra-long (100-255 chars)", (rng, i) -> randomToken(rng, 100, 255));
    }

    // ---- Prefix token tests ----

    public void testClusterReloadPrefixShort() throws Exception {
        runBenchmarkWithThresholdSearch("prefix short (syn_N)", (rng, i) -> "syn_" + i);
    }

    public void testClusterReloadPrefixLong() throws Exception {
        runBenchmarkWithThresholdSearch("prefix long (synonym_token_N)", (rng, i) -> "synonym_token_" + i);
    }

    public void testClusterReloadPrefixMulti() throws Exception {
        String[] prefixes = { "color_", "size_", "shape_" };
        runBenchmarkWithThresholdSearch("prefix multi (color_/size_/shape_ N)", (rng, i) -> prefixes[i % prefixes.length] + i);
    }

    // ---- Core benchmark logic ----

    private void runBenchmarkWithThresholdSearch(String label, TokenGenerator tokenGenerator) throws Exception {
        Path config = internalCluster().getInstance(Environment.class).configDir();
        Path synonymsFile = config.resolve(SYNONYMS_FILE);

        writeSynonymFile(synonymsFile, 1, tokenGenerator);

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

        logger.info("=== Cluster Reload Benchmark: {} (start={}, step={}) ===", label, START_TOKENS, STEP);
        logHeader();

        int lastGood = 0;

        for (int tokenCount = START_TOKENS; tokenCount <= MAX_TOKENS; tokenCount += STEP) {
            ReloadResult result = tryReload(synonymsFile, tokenCount, tokenGenerator, memoryBean);
            logResult(tokenCount, result);

            if (result.failed) {
                logger.info("=== Benchmark Complete: {} — failed at {}, last OK at {} ===", label, tokenCount, lastGood);
                return;
            }
            lastGood = tokenCount;
        }

        logger.info("=== Benchmark Complete: {} — no failure up to {} ===", label, MAX_TOKENS);
    }

    private record ReloadResult(long writeMs, long reloadMs, long heapUsedMb, long heapDeltaMb, String status, boolean failed) {}

    private ReloadResult tryReload(Path synonymsFile, int tokenCount, TokenGenerator tokenGenerator, MemoryMXBean memoryBean)
        throws IOException {
        long writeStart = System.nanoTime();
        writeSynonymFile(synonymsFile, tokenCount, tokenGenerator);
        long writeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writeStart);

        System.gc();
        long heapBefore = memoryBean.getHeapMemoryUsage().getUsed();

        long reloadStart = System.nanoTime();
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
                return new ReloadResult(writeMs, reloadMs, heapUsedMb, heapDeltaMb, "SHARD_FAIL: " + rootCauseMessage(cause), true);
            }

            String status = reloadMs > 30_000 ? "SLOW (>30s)" : "OK";
            boolean failed = reloadMs > 30_000;
            return new ReloadResult(writeMs, reloadMs, heapUsedMb, heapDeltaMb, status, failed);
        } catch (Exception e) {
            long reloadMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reloadStart);
            long heapUsedMb = memoryBean.getHeapMemoryUsage().getUsed() / (1024 * 1024);
            return new ReloadResult(writeMs, reloadMs, heapUsedMb, -1, rootCauseMessage(e), true);
        }
    }

    private void logHeader() {
        logger.info("Tokens   | Write (ms) | Reload (ms) | Tokens/ms | Heap Used | Heap Delta | Status");
        logger.info("---------|------------|-------------|-----------|-----------|------------|-------");
    }

    private void logResult(int tokenCount, ReloadResult r) {
        long tokensPerMs = r.reloadMs > 0 ? tokenCount / r.reloadMs : 0;
        String heapDelta = r.heapDeltaMb >= 0 ? r.heapDeltaMb + " MB" : "?";
        logger.info(
            "{} | {} | {} | {} | {} MB | {} | {}",
            tokenCount,
            r.writeMs,
            r.reloadMs,
            tokensPerMs,
            r.heapUsedMb,
            heapDelta,
            r.status
        );
    }

    // ---- Helpers ----

    @FunctionalInterface
    private interface TokenGenerator {
        String generate(Random rng, int index);
    }

    private static void writeSynonymFile(Path path, int targetTokens, TokenGenerator tokenGenerator) throws IOException {
        Random rng = new Random(42);
        int lines = Math.max(1, targetTokens / 3);
        int tokenIndex = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            for (int i = 0; i < lines; i++) {
                writer.write(tokenGenerator.generate(rng, tokenIndex++));
                writer.write(", ");
                writer.write(tokenGenerator.generate(rng, tokenIndex++));
                writer.write(", ");
                writer.write(tokenGenerator.generate(rng, tokenIndex++));
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
