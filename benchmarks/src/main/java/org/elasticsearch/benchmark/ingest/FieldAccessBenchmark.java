/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.ingest;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class FieldAccessBenchmark {

    IngestDocument ingestDocument = createIngestDocument(4);

    @Benchmark
    public void dotNotationAccess(Blackhole blackhole) {
        blackhole.consume(ingestDocument.getFieldValue("level1.level2.level3.level4", String.class));
    }

    @Benchmark
    public void dottedFieldAccess(Blackhole blackhole) {
        // printPaths(ingestDocument.getSource(), new ArrayList<>());
        blackhole.consume(ingestDocument.getAllFieldValues("level1.level2.level3.level4", String.class));
    }

    @SuppressWarnings("unchecked")
    private static void printPaths(Map<String, Object> source, ArrayList<String> path) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            path.add(entry.getKey());
            if (entry.getValue() instanceof Map) {
                printPaths((Map<String, Object>) entry.getValue(), path);
            } else {
                System.out.println(String.join("|", path));
            }
            path.removeLast();
        }
    }

    public static IngestDocument createIngestDocument(int maxDepth) {
        if (maxDepth < 1) {
            throw new IllegalArgumentException("Depth must be at least 1");
        }

        // create a path with the given depth
        StringBuilder path = new StringBuilder("level1");
        for (int i = 2; i <= maxDepth; i++) {
            path.append(".level").append(i);
        }
        Map<String, Object> source = new HashMap<>();
        generatePermutations(source, path.toString(), 0);
        return new IngestDocument("index", "id", 1L, "routing", VersionType.INTERNAL, source);
    }

    private static void generatePermutations(Map<String, Object> parentNode, String path, int startIndex) {
        int nextDot = path.indexOf('.', startIndex);
        if (nextDot > 0) {
            // Consider the next dot as a path separator and generate paths recursively for the rest of the path
            String key = path.substring(0, nextDot);
            Map<String, Object> child = new HashMap<>();
            parentNode.put(key, child);
            generatePermutations(child, path.substring(nextDot + 1), 0);

            // Consider the next dot as part of the path element's name
            generatePermutations(parentNode, path, nextDot + 1);
        } else {
            parentNode.put(path, "value" + nextDot * path.length());
        }
    }
}
