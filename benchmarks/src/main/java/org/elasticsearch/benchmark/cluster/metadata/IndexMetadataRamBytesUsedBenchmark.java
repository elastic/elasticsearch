/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.cluster.metadata;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@link ProjectMetadata#ramBytesUsed()} over a few thousand indices.
 * <p>
 * {@link IndexMetadata#ramBytesUsed()} is memoized, so a typical {@code clusterChanged} walk hits
 * cached per-index values. {@code @Benchmark} times that warm path. {@code @Setup} prints one
 * {@code [cold]} sample of the first call (new master / first apply).
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class IndexMetadataRamBytesUsedBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    @Param({ "1000", "5000", "10000" })
    public int indices;

    @Param({ "none", "shared" })
    public String mappingMode;

    private ProjectMetadata project;

    @Setup
    public void setUp() {
        MappingMetadata sharedMapping = switch (mappingMode) {
            case "none" -> null;
            case "shared" -> sharedKeywordMapping();
            default -> throw new IllegalArgumentException("unknown mappingMode [" + mappingMode + "]");
        };

        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        for (int i = 0; i < indices; i++) {
            IndexMetadata.Builder index = IndexMetadata.builder("idx-" + i).settings(settings).numberOfShards(1).numberOfReplicas(0);
            if (sharedMapping != null) {
                index.putMapping(sharedMapping);
            }
            builder.put(index);
        }
        project = builder.build();

        long start = System.nanoTime();
        long estimate = project.ramBytesUsed();
        long coldNs = System.nanoTime() - start;
        System.out.printf(Locale.ROOT, "[cold] indices=%d mappingMode=%s ns=%,d estimate=%,d B%n", indices, mappingMode, coldNs, estimate);
    }

    @Benchmark
    public void projectRamBytesUsedWarm(Blackhole bh) {
        bh.consume(project.ramBytesUsed());
    }

    private static MappingMetadata sharedKeywordMapping() {
        try {
            return new MappingMetadata(CompressedXContent.fromJSON("""
                { "_doc": { "properties": { "field": { "type": "keyword" } } } }
                """));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
