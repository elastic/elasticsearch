/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.script;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Measures the per-execution overhead of Painless allocation tracking across three modes:
 * <ul>
 *   <li>{@code off} – no tracking, baseline bytecode (the default today)</li>
 *   <li>{@code metrics} – per-site pre-checks + a recording histogram at execute-return; no limit enforced</li>
 *   <li>{@code limit} – per-site pre-checks + limit enforcement at 512 mb (never trips on these scripts)</li>
 * </ul>
 *
 * <p>The {@code metrics} mode installs a {@link LongHistogram} backed by a {@link LongAdder} via reflection
 * into the loaded classloader's {@code AllocationMetrics.instance}. This prevents the JIT from dead-code-
 * eliminating the {@code $allocBytes} tracking chain (which it would do with the NOOP backend, since a NOOP
 * {@code record()} has no observable side effects). The {@link LongAdder} is a realistic stand-in for the
 * production OTEL histogram backend, which also does atomic counter operations.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :benchmarks:run --args='PainlessAllocationTrackingBenchmark'
 * </pre>
 */
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class PainlessAllocationTrackingBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Tracking mode: {@code off}, {@code metrics}, or {@code limit}. */
    @Param({ "off", "metrics", "limit" })
    private String mode;

    /**
     * Script workload:
     * <ul>
     *   <li>{@code trivial} – zero allocation sites; isolates fixed per-execute overhead</li>
     *   <li>{@code allocating} – 10 statically-typed string-concat iterations (PR 5 path)</li>
     *   <li>{@code contains} – {@code List.of(...).contains(...)}; realistic ingest pattern</li>
     *   <li>{@code complex} – mixed annotated ctors ({@code HashMap}, {@code ArrayList}×5),
     *       {@code String+int} concat, entrySet iterator, and {@code String+String} concat</li>
     *   <li>{@code def_alloc} – def-typed string concat (PR 7.5 MIC path) and def method
     *       dispatch to annotated targets (PR 7 PIC path)</li>
     * </ul>
     */
    @Param({ "trivial", "allocating", "contains", "complex", "def_alloc" })
    private String script;

    private IngestConditionalScript compiledScript;

    @Setup
    public void setup() throws Exception {
        if ("metrics".equals(mode)) {
            System.setProperty("es.painless.allocation.metrics.enabled", "true");
        }
        URLClassLoader loader;
        try {
            Settings settings;
            if ("limit".equals(mode)) {
                // 512 mb — far above anything these scripts allocate; enforces the per-site bytecode
                // path without ever throwing PainlessAllocationLimitError during the benchmark.
                settings = Settings.builder()
                    .put("script.painless.max_allocation_bytes.context.processor_conditional.limit", "512mb")
                    .build();
            } else {
                settings = Settings.EMPTY;
            }

            EngineAndLoader result = loadPainlessEngine(settings);
            loader = result.loader();
            ScriptEngine engine = result.engine();

            if ("metrics".equals(mode)) {
                // Install a LongAdder-backed histogram so the JIT cannot dead-code-eliminate the
                // $allocBytes tracking chain. With MeterRegistry.NOOP the record() call is an empty
                // method, and the JIT inlines + eliminates the entire chain back to the $allocBytes
                // field writes. The LongAdder.add() has real memory-ordering semantics, preventing that.
                installRecordingMetrics(loader);
            }

            String source = switch (script) {
                // Zero allocation sites. Measures: counter reset at entry + histogram record at return.
                case "trivial" -> "return true";
                // Ten string-concat iterations, each emitting a $checkAllocBytes pre-check.
                case "allocating" -> """
                    String s = '';
                    for (int i = 0; i < 10; i++) {
                        s += 'hello ';
                    }
                    return s.length() > 0""";
                // List.of(5 elems) + iterator allocation on every execute. Realistic ingest pattern.
                case "contains" -> "return ['alfa', 'bravo', 'charlie', 'delta', 'echo'].contains(params.word)";
                // Mixed: new HashMap + new ArrayList×5 + String+int concat×5 + entrySet iterator
                // + String+String concat×5. Exercises annotated ctors and multiple concat sites.
                case "complex" -> """
                    Map m = new HashMap();
                    for (int i = 0; i < 5; i++) {
                        m.put('key' + i, new ArrayList());
                    }
                    String result = '';
                    for (def entry : m.entrySet()) {
                        result += entry.getKey() + ' ';
                    }
                    return result.length() > 0""";
                // def+def string concat (PR 7.5 MIC bootstrap) + def method dispatch to annotated
                // targets (PR 7 PIC path). Exercises runtime-resolved tracking paths.
                case "def_alloc" -> """
                    def s = '';
                    for (int i = 0; i < 5; i++) {
                        def piece = 'hello ';
                        s = s + piece;
                    }
                    def list = new ArrayList();
                    for (int i = 0; i < 5; i++) {
                        list.add('item' + i);
                    }
                    def joined = '';
                    for (def item : list) {
                        joined = joined + item;
                    }
                    return joined.length() > 0""";
                default -> throw new IllegalArgumentException("unknown script: " + script);
            };

            IngestConditionalScript.Factory factory = engine.compile(
                "alloc-bench-" + script,
                source,
                IngestConditionalScript.CONTEXT,
                Map.of()
            );

            Map<String, Object> params = new HashMap<>();
            params.put("word", "echo");
            Map<String, Object> ctxMap = new HashMap<>();
            ctxMap.put("message", "test");

            compiledScript = factory.newInstance(params, ctxMap);
        } finally {
            System.clearProperty("es.painless.allocation.metrics.enabled");
        }
    }

    @Benchmark
    public boolean benchmark() {
        return compiledScript.execute();
    }

    /**
     * Installs a {@link LongAdder}-backed {@code AllocationMetrics} into the plugin classloader's
     * static {@code AllocationMetrics.instance} field. Called only in {@code metrics} mode.
     *
     * <p>{@code MeterRegistry} and {@code LongHistogram} are interfaces in the parent classloader
     * (server module), visible to the plugin classloader via normal parent-delegation. The proxy
     * classes created here are therefore assignable to the types expected by {@code AllocationMetrics}.
     */
    private static void installRecordingMetrics(URLClassLoader loader) throws Exception {
        LongAdder adder = new LongAdder();

        // A LongHistogram that accumulates into a LongAdder — real memory-ordering side effects.
        LongHistogram recordingHistogram = (LongHistogram) Proxy.newProxyInstance(
            LongHistogram.class.getClassLoader(),
            new Class<?>[] { LongHistogram.class },
            (proxy, method, args) -> {
                if ("record".equals(method.getName())) {
                    adder.add((long) args[0]);
                }
                return null;
            }
        );

        // A MeterRegistry that returns the recording histogram for registerLongHistogram,
        // and delegates everything else to NOOP.
        MeterRegistry recordingRegistry = (MeterRegistry) Proxy.newProxyInstance(
            MeterRegistry.class.getClassLoader(),
            new Class<?>[] { MeterRegistry.class },
            (proxy, method, args) -> {
                if ("registerLongHistogram".equals(method.getName())) {
                    return recordingHistogram;
                }
                return method.invoke(MeterRegistry.NOOP, args);
            }
        );

        Class<?> allocMetricsClass = loader.loadClass("org.elasticsearch.painless.AllocationMetrics");
        Object metricsInstance = allocMetricsClass.getDeclaredConstructor(MeterRegistry.class).newInstance(recordingRegistry);
        allocMetricsClass.getMethod("setInstance", allocMetricsClass).invoke(null, metricsInstance);
    }

    private static EngineAndLoader loadPainlessEngine(Settings settings) throws Exception {
        Path pluginDir = Path.of(System.getProperty("plugins.dir"), "painless");
        URL[] jarUrls;
        try (var stream = Files.walk(pluginDir)) {
            jarUrls = stream.filter(p -> p.toString().endsWith(".jar")).map(p -> {
                try {
                    return p.toUri().toURL();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).toArray(URL[]::new);
        }
        URLClassLoader loader = URLClassLoader.newInstance(jarUrls, PainlessAllocationTrackingBenchmark.class.getClassLoader());
        Class<?> pluginClass = loader.loadClass("org.elasticsearch.painless.PainlessPlugin");
        Object plugin = pluginClass.getDeclaredConstructor().newInstance();
        ((ExtensiblePlugin) plugin).loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return List.of();
            }
        });
        ScriptPlugin scriptPlugin = (ScriptPlugin) plugin;
        ScriptModule scriptModule = new ScriptModule(settings, List.of(scriptPlugin));
        return new EngineAndLoader(scriptModule.engines.get("painless"), loader);
    }

    private record EngineAndLoader(ScriptEngine engine, URLClassLoader loader) {}
}
