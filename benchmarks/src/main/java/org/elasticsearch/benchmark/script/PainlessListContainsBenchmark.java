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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Benchmarks the common Painless pattern of {@code ['a','b','c'].contains(x)}
 * used pervasively in ingest pipeline {@code if:} conditions.
 *
 * Measures baseline performance across a matrix of list sizes and hit/miss lookups.
 */
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class PainlessListContainsBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final List<String> WORDS = List.of(
        "alfa",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliett",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa",
        "quebec",
        "romeo",
        "sierra",
        "tango",
        "uniform",
        "victor",
        "whiskey",
        "xray",
        "yankee",
        "zulu",
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "eleven",
        "twelve",
        "thirteen",
        "fourteen"
    );

    private static final String MISS_WORD = "ZZZNOTFOUND";

    @Param({ "5", "15", "40" })
    private int listSize;

    @Param({ "hit", "miss" })
    private String mode;

    private IngestConditionalScript script;

    @Setup
    public void setup() throws Exception {
        ScriptEngine engine = loadPainlessEngine();

        List<String> words = WORDS.subList(0, listSize);

        String listLiteral = words.stream().map(w -> "'" + w + "'").collect(Collectors.joining(", ", "[", "]"));
        String source = listLiteral + ".contains(params.target)";

        IngestConditionalScript.Factory factory = engine.compile("bench", source, IngestConditionalScript.CONTEXT, Map.of());

        String target = "hit".equals(mode) ? words.get(words.size() - 1) : MISS_WORD;
        Map<String, Object> params = new HashMap<>();
        params.put("target", target);

        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("message", "test");

        script = factory.newInstance(params, ctxMap);
    }

    @Benchmark
    public boolean benchmark() {
        return script.execute();
    }

    private static ScriptEngine loadPainlessEngine() throws Exception {
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
        URLClassLoader loader = URLClassLoader.newInstance(jarUrls, PainlessListContainsBenchmark.class.getClassLoader());
        Class<?> pluginClass = loader.loadClass("org.elasticsearch.painless.PainlessPlugin");
        Object plugin = pluginClass.getDeclaredConstructor().newInstance();
        ((ExtensiblePlugin) plugin).loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return List.of();
            }
        });
        ScriptPlugin scriptPlugin = (ScriptPlugin) plugin;
        ScriptModule scriptModule = new ScriptModule(Settings.EMPTY, List.of(scriptPlugin));
        return scriptModule.engines.get("painless");
    }
}
