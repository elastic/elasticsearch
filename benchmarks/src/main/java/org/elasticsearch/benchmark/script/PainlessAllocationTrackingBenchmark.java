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
 * <p>The {@code metrics} mode installs a {@link LongHistogram} backed by a {@link LongAdder}, by calling the plugin's
 * {@code createComponents} with a {@link MeterRegistry} that hands one out — the same path a node takes. This prevents the
 * JIT from dead-code-eliminating the {@code $allocBytes} tracking chain (which it would do with the NOOP backend, since a
 * NOOP {@code record()} has no observable side effects). The {@link LongAdder} is a realistic stand-in for the production
 * OTEL histogram backend, which also does atomic counter operations.
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
     * Mirrors {@code CompilerSettings.ALLOCATION_METRICS_ENABLED_PROPERTY}, which lives in the plugin classloader and so
     * cannot be referenced directly from here. Keep the two in step: a mismatch silently turns {@code metrics} mode into a
     * second copy of {@code off} rather than failing.
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
                // 512 mb — far above anything these scripts allocate; enforces the per-site bytecode
                // path without ever throwing PainlessAllocationLimitError during the benchmark.
                settings = Settings.builder()
                    .put("script.painless.max_allocation_bytes.context.processor_conditional.limit", "512mb")
                    .build();
            } else {
                settings = Settings.EMPTY;
            }

            EngineAndPlugin result = loadPainlessEngine(settings);
            ScriptEngine engine = result.engine();

            if ("metrics".equals(mode)) {
                // Install a LongAdder-backed histogram so the JIT cannot dead-code-eliminate the
                // $allocBytes tracking chain. With MeterRegistry.NOOP the record() call is an empty
                // method, and the JIT inlines + eliminates the entire chain back to the $allocBytes
                // field writes. The LongAdder.add() has real memory-ordering semantics, preventing that.
                // Must run before the compile below, which is what reads the installed instance.
                installRecordingMetrics(result.plugin());
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
            System.clearProperty(ALLOCATION_METRICS_ENABLED_PROPERTY);
        }
    }

    @Benchmark
    public boolean benchmark() {
        return compiledScript.execute();
    }

    /**
     * Installs a {@link LongAdder}-backed {@code AllocationMetrics} by driving the plugin's real wiring:
     * {@code createComponents} is the hook that hands it a {@link MeterRegistry}, so this feeds it one that records.
     * Called only in {@code metrics} mode, and only after the engine exists — the engine reads the installed instance once
     * per compile, so this must land before the script is compiled.
     *
     * <p>{@code MeterRegistry}, {@code LongHistogram}, {@code TelemetryProvider} and {@code PluginServices} are all
     * interfaces in the parent classloader (server module), visible to the plugin classloader via normal parent-delegation.
     * The proxy classes created here are therefore assignable to the types the plugin expects. {@code PainlessPlugin}
     * touches only {@code telemetryProvider()}, so the rest of {@code PluginServices} can answer null.
     */
    private static void installRecordingMetrics(Plugin plugin) {
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

        // A TelemetryProvider handing out that registry, and the PluginServices that carries it into createComponents.
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
}
