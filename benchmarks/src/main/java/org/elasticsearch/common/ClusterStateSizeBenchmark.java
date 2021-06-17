/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@Fork(0)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class ClusterStateSizeBenchmark {

    @State(Scope.Benchmark)
    public static class MyState {
        public ClusterState state;

        @Setup
        public void setup() throws IOException {
            byte[] bytes = ClusterStateSizeBenchmark.class.getResourceAsStream("/a.json").readAllBytes();
            ImmutableOpenMap.Builder<String, IndexTemplateMetadata> builder = ImmutableOpenMap.builder();
            for (int i = 0; i < 1000; i++) {
                XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes);
                builder.put("a" + i, IndexTemplateMetadata.Builder.fromXContent(parser, "a" + i));
            }
            state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().templates(builder.build())).build();
        }
    }

    public static class SizeOutputStream extends OutputStream {

        public long size;

        @Override
        public void write(int b) throws IOException {
            size++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            size += len;
        }
    }

    @Benchmark
    public long size(MyState state) throws IOException {
        SizeOutputStream sos = new SizeOutputStream();
        XContentBuilder builder = new XContentBuilder(XContentType.SMILE.xContent(), sos);
        state.state.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return sos.size;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(ClusterStateSizeBenchmark.class.getSimpleName())
            .forks(1)
            .build();

        new Runner(opt).run();
    }
}
