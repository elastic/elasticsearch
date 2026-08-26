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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.telemetry.TelemetryProvider;
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
 * <p>{@code metrics} mode installs a {@link LongAdder}-backed {@link LongHistogram} through the plugin's
 * {@code createComponents}, the same path a node takes. A NOOP {@code record()} has no observable side effects, so the JIT
 * would eliminate the whole {@code $allocBytes} chain; {@link LongAdder} is a fair stand-in for the OTEL backend, which
 * also does atomic counter work.
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

    /**
     * Mirrors {@code CompilerSettings.ALLOCATION_METRICS_ENABLED_PROPERTY}, unreachable from here in the plugin
     * classloader. Keep them in step: a mismatch silently makes {@code metrics} a second copy of {@code off}.
     */
    private static final String ALLOCATION_METRICS_ENABLED_PROPERTY = "es.painless.allocation_metrics.enabled";

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
            System.setProperty(ALLOCATION_METRICS_ENABLED_PROPERTY, "true");
        }
        try {
            Settings settings;
            if ("limit".equals(mode)) {
                // Far above anything these scripts allocate: emits the per-site checks without ever tripping.
                settings = Settings.builder()
                    .put("script.painless.max_allocation_bytes.context.processor_conditional.limit", "512mb")
                    .build();
            } else {
                settings = Settings.EMPTY;
            }

            EngineAndPlugin result = loadPainlessEngine(settings);
            ScriptEngine engine = result.engine();

            if ("metrics".equals(mode)) {
                // Before the compile below, which is what reads the installed instance.
                installRecordingMetrics(result.plugin());
            }

            String source = switch (script) {
                // Zero allocation sites: measures the counter reset at entry and the record at return.
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
                // Annotated ctors, an entrySet iterator, and several concat sites in one script.
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
                // The runtime-resolved paths: def+def concat and def dispatch to annotated targets.
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
            System.clearProperty(ALLOCATION_METRICS_ENABLED_PROPERTY);
        }
    }

    @Benchmark
    public boolean benchmark() {
        return compiledScript.execute();
    }

    /**
     * Installs a {@link LongAdder}-backed {@code AllocationMetrics} through {@code createComponents}, the hook that hands
     * the plugin a {@link MeterRegistry}. Must run before the script is compiled, which is when the instance is read.
     *
     * <p>All four interfaces proxied here live in the parent (server) classloader, so the proxies are assignable to what
     * the plugin expects. {@code PainlessPlugin} touches only {@code telemetryProvider()}, so the rest can answer null.
     */
    private static void installRecordingMetrics(Plugin plugin) {
        LongHistogram recordingHistogram = new RecordingHistogram();

        // A registry handing out that histogram, delegating everything else to NOOP.
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

        // And the two layers that carry it into createComponents.
        TelemetryProvider recordingTelemetry = (TelemetryProvider) Proxy.newProxyInstance(
            TelemetryProvider.class.getClassLoader(),
            new Class<?>[] { TelemetryProvider.class },
            (proxy, method, args) -> "getMeterRegistry".equals(method.getName())
                ? recordingRegistry
                : method.invoke(TelemetryProvider.NOOP, args)
        );

        Plugin.PluginServices services = (Plugin.PluginServices) Proxy.newProxyInstance(
            Plugin.PluginServices.class.getClassLoader(),
            new Class<?>[] { Plugin.PluginServices.class },
            (proxy, method, args) -> "telemetryProvider".equals(method.getName()) ? recordingTelemetry : null
        );

        plugin.createComponents(services);
    }

    private static EngineAndPlugin loadPainlessEngine(Settings settings) throws Exception {
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
        return new EngineAndPlugin(scriptModule.engines.get("painless"), (Plugin) plugin);
    }

    /** The plugin is kept because {@code metrics} mode installs its recording metrics through it after the engine exists. */
    private record EngineAndPlugin(ScriptEngine engine, Plugin plugin) {}

    /**
     * Accumulates into a {@link LongAdder} so the record call has real side effects and cannot be optimized away. A direct
     * implementation rather than a {@link Proxy}: proxy dispatch costs more than the call being measured, and this sits on
     * the hot path once per execution.
     */
    private static class RecordingHistogram implements LongHistogram {
        private final LongAdder adder = new LongAdder();

        @Override
        public String getName() {
            return "benchmark";
        }

        @Override
        public void record(long value) {
            adder.add(value);
        }

        @Override
        public void record(long value, Map<String, Object> attributes) {
            adder.add(value);
        }
    }
}
