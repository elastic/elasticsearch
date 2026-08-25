/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Compares simdjson-backed vs Jackson-backed parsing through the full {@link EscfEncoder} pipeline,
 * matching the actual bulk indexing usage pattern: parse, flatten, stage into columnar row buffer,
 * commit, and build the batch.
 *
 * <p>Benchmark methods exercise the same documents through different parser paths:
 * <ul>
 *   <li>{@code simdJsonEncode} — uses the default {@link EscfEncoder} which dispatches to the
 *       direct walker (SIMD stage 1 + fused walk) for eligible documents.</li>
 *   <li>{@code simdJsonDirectEncode} — uses the batch API with the direct walker.</li>
 *   <li>{@code jacksonEncode} — uses an {@link EscfEncoder} with SIMD disabled, forcing all
 *       documents through Jackson's {@code ESUTF8StreamJsonParser}.</li>
 * </ul>
 *
 * <p>A fresh encoder is created per invocation (matching production lifecycle where one encoder
 * is created per bulk request per concrete index).
 *
 * <p><strong>Running.</strong>
 * <pre>{@code
 * cd benchmarks
 * # Single-threaded (default):
 * ../gradlew run --args "org.elasticsearch.benchmark.xcontent.SimdJsonParserBenchmark \
 *   -rf json -rff build/jmh-result.json" | tee /tmp/bench/simdjson_vs_jackson
 *
 * # Multi-threaded (8 threads):
 * ../gradlew run --args "org.elasticsearch.benchmark.xcontent.SimdJsonParserBenchmark \
 *   -t 8 -rf json -rff build/jmh-result.json" | tee /tmp/bench/simdjson_vs_jackson_mt
 * }</pre>
 */
@Fork(value = 1, jvmArgsAppend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(Threads.MAX)
@State(Scope.Thread)
public class SimdJsonParserBenchmark {

    private static final MethodHandle ESCF_ENCODER_CTOR;
    static {
        try {
            var lookup = MethodHandles.privateLookupIn(EscfEncoder.class, MethodHandles.lookup());
            ESCF_ENCODER_CTOR = lookup.findConstructor(
                EscfEncoder.class,
                MethodType.methodType(void.class, org.elasticsearch.common.recycler.Recycler.class, boolean.class)
            );
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Param({ "10000" })
    private int docCount;

    @Param({ "42" })
    private long seed;

    @Param({ "clickbench_flat", "otel_nested", "small_sparse" })
    private String shape;

    private BytesReference[] docs;

    private byte[] batchBuffer;
    private int[] batchOffsets;
    private int[] batchLens;

    @Setup
    public void setUp() {
        Utils.configureBenchmarkLogging();
        long threadSeed = seed + Thread.currentThread().threadId();
        Random random = new Random(threadSeed);
        docs = new BytesReference[docCount];
        int minLen = Integer.MAX_VALUE, maxLen = 0;
        long totalLen = 0;
        for (int i = 0; i < docCount; i++) {
            byte[] raw = generateDoc(random, shape, i).getBytes(UTF_8);
            docs[i] = new BytesArray(raw);
            minLen = Math.min(minLen, raw.length);
            maxLen = Math.max(maxLen, raw.length);
            totalLen += raw.length;
        }

        batchOffsets = new int[docCount];
        batchLens = new int[docCount];
        batchBuffer = new byte[(int) totalLen];
        int pos = 0;
        for (int i = 0; i < docCount; i++) {
            byte[] raw = docs[i].toBytesRef().bytes;
            int len = docs[i].length();
            batchOffsets[i] = pos;
            batchLens[i] = len;
            System.arraycopy(raw, 0, batchBuffer, pos, len);
            pos += len;
        }

        boolean nativeAvailable;
        try {
            var m = Class.forName("org.elasticsearch.simdjson.SimdJsonSupport").getDeclaredMethod("isSupported");
            nativeAvailable = (boolean) m.invoke(null);
        } catch (Exception e) {
            nativeAvailable = false;
        }
        System.out.printf(
            "[setup] thread=%s shape=%s docCount=%d docSize min=%d avg=%d max=%d nativeStage1=%s maxSimdDocBytes=%d%n",
            Thread.currentThread().getName(),
            shape,
            docCount,
            minLen,
            totalLen / docCount,
            maxLen,
            nativeAvailable,
            16 * 1024
        );
    }

    @Benchmark
    public int jacksonEncode() throws IOException {
        try (EscfEncoder encoder = newEncoder(false)) {
            for (BytesReference doc : docs) {
                encoder.parseToScratch(doc, XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            try (EscfBatch batch = encoder.buildPartition(0)) {
                return batch.schema().leafCount();
            }
        }
    }

    @Benchmark
    public int simdJsonEncode() throws IOException {
        try (EscfEncoder encoder = new EscfEncoder(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            for (BytesReference doc : docs) {
                encoder.parseToScratch(doc, XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            EscfEncoder.releaseWalkerNames();
            try (EscfBatch batch = encoder.buildPartition(0)) {
                return batch.schema().leafCount();
            }
        }
    }

    @Benchmark
    public int simdJsonBatchEncode() throws IOException {
        try (EscfEncoder encoder = new EscfEncoder(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            encoder.parseBatchDirect(batchBuffer, batchOffsets, batchLens, docCount, 0, LeafSink.NO_OP);
            EscfEncoder.releaseWalkerNames();
            try (EscfBatch batch = encoder.buildPartition(0)) {
                return batch.schema().leafCount();
            }
        }
    }

    private static EscfEncoder newEncoder(boolean allowSimd) {
        try {
            return (EscfEncoder) ESCF_ENCODER_CTOR.invoke(BytesRefRecycler.NON_RECYCLING_INSTANCE, allowSimd);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------------
    // Document generators (same shapes as EscfFieldResolutionBenchmark)
    // ------------------------------------------------------------------

    private static String generateDoc(Random random, String shape, int docIndex) {
        return switch (shape) {
            case "clickbench_flat" -> generateClickBenchFlat(random);
            case "otel_nested" -> generateOtelNested(random);
            case "small_sparse" -> generateSmallSparse(random, docIndex);
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    private static String generateClickBenchFlat(Random random) {
        return """
            {
              "WatchID": %d, "JavaEnable": %d, "Title": "%s",
              "GoodEvent": %d, "EventTime": %d, "EventDate": %d,
              "CounterID": %d, "ClientIP": %d, "ClientIP6": "%s",
              "RegionID": %d, "UserID": %d,
              "CounterClass": %d, "OS": %d, "UserAgent": %d,
              "URL": "https://example.com/%s", "Referer": "https://ref.example.com/%s",
              "URLDomain": "example.com", "RefererDomain": "ref.example.com",
              "Refresh": %d, "IsRobot": %d, "RefererCategories": %d,
              "URLCategories": %d, "URLRegions": %d, "RefererRegions": %d,
              "ResolutionWidth": %d, "ResolutionHeight": %d, "ResolutionDepth": %d,
              "FlashMajor": %d, "FlashMinor": %d, "FlashMinor2": "%d",
              "NetMajor": %d, "NetMinor": %d, "UserAgentMajor": %d,
              "UserAgentMinor": %d, "CookieEnable": %d, "JavascriptEnable": %d,
              "IsMobile": %d, "MobilePhone": %d, "MobilePhoneModel": "%s",
              "Params": "", "IPNetworkID": %d,
              "TraficSourceID": %d, "SearchEngineID": %d,
              "SearchPhrase": "%s",
              "AdvEngineID": %d, "IsArtifical": %d, "WindowClientWidth": %d,
              "WindowClientHeight": %d, "ClientTimeZone": %d,
              "ClientEventTime": %d, "SilverlightVersion1": %d, "SilverlightVersion2": %d,
              "SilverlightVersion3": %d, "SilverlightVersion4": %d,
              "PageCharset": "UTF-8", "CodeVersion": %d, "IsLink": %d,
              "IsDownload": %d, "IsNotBounce": %d, "FUniqID": %d,
              "HID": %d, "IsOldCounter": %d, "IsEvent": %d,
              "IsParameter": %d, "DontCountHits": %d, "WithHash": %d,
              "HitColor": "W", "UTCEventTime": %d,
              "Age": %d, "Sex": %d, "Income": %d,
              "Interests": %d, "Robotness": %d, "GeneralInterests": %d,
              "RemoteIP": %d, "RemoteIP6": "%s",
              "WindowName": %d, "OpenerName": %d, "HistoryLength": %d,
              "BrowserLanguage": "en", "BrowserCountry": "US",
              "SocialNetwork": "", "SocialAction": "", "HTTPError": %d,
              "SendTiming": %d, "DNSTiming": %d, "ConnectTiming": %d,
              "ResponseStartTiming": %d, "ResponseEndTiming": %d,
              "FetchTiming": %d, "RedirectTiming": %d, "DOMInteractiveTiming": %d,
              "ContentLoadTiming": %d, "OnLoadTiming": %d,
              "RequestNum": %d, "RequestTry": %d,
              "NetErrorCode": %d, "SocialShareNetwork": "", "SocialSharePage": "",
              "ParamPrice": %d, "ParamOrderID": "", "ParamCurrency": "USD",
              "ParamCurrencyID": %d,
              "GoalsReached": %d, "OpenstatServiceName": "", "OpenstatCampaignID": "",
              "OpenstatAdID": "", "OpenstatSourceID": "",
              "UTMSource": "", "UTMMedium": "", "UTMCampaign": "", "UTMContent": "", "UTMTerm": "",
              "FromTag": "", "HasGCLID": %d, "RefererHash": %d, "URLHash": %d,
              "CLID": %d, "YCLID": %d, "ShareService": "", "ShareURL": "", "ShareTitle": ""
            }""".formatted(
            random.nextLong(),
            random.nextInt(2),
            randomWord(random),
            random.nextInt(2),
            random.nextLong(),
            random.nextInt(19000),
            random.nextInt(100000),
            (long) (random.nextDouble() * 4_294_967_295L),
            "::1",
            random.nextInt(200000),
            random.nextLong(),
            random.nextInt(10),
            random.nextInt(255),
            random.nextInt(255),
            randomWord(random),
            randomWord(random),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(1000),
            random.nextInt(1000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(3840),
            random.nextInt(2160),
            random.nextInt(32),
            random.nextInt(33),
            random.nextInt(10),
            random.nextInt(10),
            random.nextInt(10),
            random.nextInt(10),
            random.nextInt(100),
            random.nextInt(100),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            randomWord(random),
            random.nextInt(1000000),
            random.nextInt(30),
            random.nextInt(100),
            randomWord(random),
            random.nextInt(10),
            random.nextInt(2),
            random.nextInt(3840),
            random.nextInt(2160),
            random.nextInt(720),
            random.nextLong(),
            random.nextInt(4),
            random.nextInt(4),
            random.nextInt(4000),
            random.nextInt(10000),
            random.nextInt(1000000),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextLong(),
            random.nextInt(1000000),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextLong(),
            random.nextInt(90),
            random.nextInt(2),
            random.nextInt(5),
            random.nextInt(10000),
            random.nextInt(10),
            random.nextInt(1000),
            (long) (random.nextDouble() * 4_294_967_295L),
            "::1",
            random.nextInt(1000),
            random.nextInt(1000),
            random.nextInt(100),
            random.nextInt(1000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100),
            random.nextInt(10),
            random.nextInt(10),
            random.nextLong(),
            random.nextInt(1000),
            random.nextInt(10),
            random.nextInt(2),
            random.nextLong(),
            random.nextLong(),
            random.nextInt(100),
            random.nextLong(),
            random.nextLong(),
            random.nextInt(100),
            random.nextLong()
        );
    }

    private static String generateOtelNested(Random random) {
        return """
            {
              "@timestamp": "2025-09-23T%02d:%02d:%02dZ",
              "resource": {
                "service.name": "%s",
                "service.version": "1.%d.0",
                "host.name": "host-%d",
                "deployment.environment": "%s"
              },
              "scope": {
                "name": "%s-logger",
                "version": "2.%d.0"
              },
              "severity_text": "%s",
              "severity_number": %d,
              "body": "%s",
              "trace_id": "%s",
              "span_id": "%s",
              "trace_flags": %d,
              "attributes": {
                "http.method": "%s",
                "http.status_code": %d,
                "http.url": "https://api.example.com/%s",
                "user.id": %d,
                "db.system": "postgresql",
                "db.statement": "SELECT * FROM %s WHERE id = %d"
              }
            }""".formatted(
            random.nextInt(24),
            random.nextInt(60),
            random.nextInt(60),
            randomService(random),
            random.nextInt(10),
            random.nextInt(100),
            randomEnv(random),
            randomService(random),
            random.nextInt(5),
            randomSeverity(random),
            random.nextInt(25),
            randomMessage(random),
            randomHex(random, 32),
            randomHex(random, 16),
            random.nextInt(2),
            randomMethod(random),
            random.nextInt(599) + 100,
            randomWord(random),
            random.nextLong(),
            randomWord(random),
            random.nextInt(10000)
        );
    }

    private static String generateSmallSparse(Random random, int docIndex) {
        return switch (docIndex % 3) {
            case 0 -> """
                {"type":"A","id":%d,"ts":%d,"val":%.4f,"label":"%s","active":%b,"count":%d}""".formatted(
                random.nextLong(),
                random.nextLong(),
                random.nextDouble(),
                randomWord(random),
                random.nextBoolean(),
                random.nextInt(10000)
            );
            case 1 -> """
                {"type":"B","uid":"%s","score":%.3f,"tags":%d,"region":"%s","retries":%d}""".formatted(
                randomWord(random),
                random.nextDouble() * 100,
                random.nextInt(50),
                randomWord(random),
                random.nextInt(5)
            );
            default -> """
                {"type":"C","key":%d,"name":"%s","bytes":%d,"ok":%b,"lat":%.2f,"code":%d}""".formatted(
                random.nextLong(),
                randomWord(random),
                random.nextLong(),
                random.nextBoolean(),
                random.nextDouble() * 1000,
                random.nextInt(600)
            );
        };
    }

    // ------------------------------------------------------------------
    // Value generators
    // ------------------------------------------------------------------

    private static final String[] WORDS = {
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa" };
    private static final String[] SERVICES = { "frontend", "backend", "gateway", "worker", "scheduler" };
    private static final String[] ENVS = { "prod", "staging", "dev", "qa" };
    private static final String[] SEVERITIES = { "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL" };
    private static final String[] MESSAGES = {
        "Request processed",
        "Connection timeout",
        "Failed to place order",
        "Slow query detected",
        "Cache miss",
        "Auth succeeded" };
    private static final String[] METHODS = { "GET", "POST", "PUT", "DELETE", "PATCH" };

    private static String randomWord(Random r) {
        return WORDS[r.nextInt(WORDS.length)];
    }

    private static String randomService(Random r) {
        return SERVICES[r.nextInt(SERVICES.length)];
    }

    private static String randomEnv(Random r) {
        return ENVS[r.nextInt(ENVS.length)];
    }

    private static String randomSeverity(Random r) {
        return SEVERITIES[r.nextInt(SEVERITIES.length)];
    }

    private static String randomMessage(Random r) {
        return MESSAGES[r.nextInt(MESSAGES.length)];
    }

    private static String randomMethod(Random r) {
        return METHODS[r.nextInt(METHODS.length)];
    }

    private static String randomHex(Random r, int digits) {
        StringBuilder sb = new StringBuilder(digits);
        for (int i = 0; i < digits; i++) {
            sb.append(HEX_CHARS[r.nextInt(16)]);
        }
        return sb.toString();
    }

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
}
