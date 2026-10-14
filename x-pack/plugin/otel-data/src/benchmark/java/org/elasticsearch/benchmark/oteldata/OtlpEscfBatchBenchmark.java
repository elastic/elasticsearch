/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark.oteldata;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfBatchScatterer;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricColumnarBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class OtlpEscfBatchBenchmark {

    private static final int METRICS_PER_ROW = 4;
    private static final int ATTRIBUTE_CARDINALITY = 32;
    private static final long BASE_TIMESTAMP_NANOS = 1_700_000_000_000_000_000L;

    @State(Scope.Benchmark)
    public static class Corpus {
        @Param({ "1000", "10000" })
        public int rows;

        @Param({ "4", "16" })
        public int attributes;

        @Param({ "42" })
        public long seed;

        BytesReference request;
        MappingHints mappingHints;
        Recycler<BytesRef> bytesRefRecycler;
        EscfBatch batch;

        @Setup
        public void setup() throws IOException {
            BenchmarkLogging.configure();
            setup(new Random(seed));
        }

        public void setup(Random random) throws IOException {
            bytesRefRecycler = new BytesRefRecycler(new PageCacheRecycler(Settings.EMPTY));
            mappingHints = MappingHints.fromSettings(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.getDefault(Settings.EMPTY));
            request = new BytesArray(exportRequest(random, rows, attributes).toByteArray());
            batch = build(request, mappingHints, bytesRefRecycler);
        }

        @TearDown
        public void tearDown() {
            batch.close();
        }
    }

    @State(Scope.Benchmark)
    public static class Partitioning {
        @Param({ "1", "4" })
        public int partitions;

        int[] partitionIds;

        @Setup
        public void setup(Corpus corpus) {
            setup(corpus, new Random(corpus.seed));
        }

        public void setup(Corpus corpus, Random random) {
            partitionIds = new int[corpus.batch.docCount()];
            for (int row = 0; row < partitionIds.length; row++) {
                partitionIds[row] = random.nextInt(partitions);
            }
        }
    }

    @Benchmark
    public void buildBatch(Corpus corpus, Blackhole bh) throws IOException {
        try (EscfBatch batch = build(corpus.request, corpus.mappingHints, corpus.bytesRefRecycler)) {
            bh.consume(batch);
        }
    }

    @Benchmark
    @Warmup(iterations = 3, time = 2)
    @Measurement(iterations = 5, time = 2)
    public void scatter(Corpus corpus, Partitioning partitioning, Blackhole bh) {
        EscfBatch[] parts = scatterParts(corpus, partitioning);
        bh.consume(parts);
        Releasables.close(parts);
    }

    @Benchmark
    @Warmup(iterations = 3, time = 2)
    @Measurement(iterations = 5, time = 2)
    public void scatterAndSerialize(Corpus corpus, Partitioning partitioning, Blackhole bh) {
        EscfBatch[] parts = scatterParts(corpus, partitioning);
        for (EscfBatch part : parts) {
            if (part != null) {
                bh.consume(part.data());
            }
        }
        Releasables.close(parts);
    }

    static EscfBatch[] scatterParts(Corpus corpus, Partitioning partitioning) {
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(corpus.bytesRefRecycler)) {
            return scatterer.scatter(corpus.batch, partitioning.partitionIds, partitioning.partitions);
        }
    }

    static EscfBatch build(BytesReference request, MappingHints mappingHints, Recycler<BytesRef> recycler) throws IOException {
        ExportMetricsServiceRequest parsed = ExportMetricsServiceRequest.parseFrom(request.streamInput());
        DataPointGroupingContext context = new DataPointGroupingContext(new BufferedByteStringAccessor(), mappingHints);
        context.groupDataPoints(parsed);
        MetricColumnarBuilder columnar = new MetricColumnarBuilder(mappingHints);
        try (EscfBatchBuilder builder = new EscfBatchBuilder(recycler)) {
            context.consume(group -> {
                if (columnar.buildMetricRow(builder, group, new HashMap<>(), new HashMap<>()) == false) {
                    throw new IllegalStateException("corpus group is not ESCF-eligible");
                }
                builder.commit(0);
            });
            return builder.buildPartition(0);
        }
    }

    static ExportMetricsServiceRequest exportRequest(Random random, int rows, int attributes) {
        List<List<KeyValue>> rowAttributes = new ArrayList<>(rows);
        for (int row = 0; row < rows; row++) {
            List<KeyValue> keyValues = new ArrayList<>(attributes);
            for (int attribute = 0; attribute < attributes; attribute++) {
                keyValues.add(stringAttribute("attr_" + attribute, "value_" + random.nextInt(ATTRIBUTE_CARDINALITY)));
            }
            rowAttributes.add(keyValues);
        }
        ScopeMetrics.Builder scope = ScopeMetrics.newBuilder()
            .setScope(InstrumentationScope.newBuilder().setName("benchmark").setVersion("1.0.0"));
        for (int metric = 0; metric < METRICS_PER_ROW; metric++) {
            Gauge.Builder gauge = Gauge.newBuilder();
            for (int row = 0; row < rows; row++) {
                NumberDataPoint.Builder point = NumberDataPoint.newBuilder()
                    .setTimeUnixNano(BASE_TIMESTAMP_NANOS + row * 1_000_000L)
                    .addAllAttributes(rowAttributes.get(row));
                if (metric % 2 == 0) {
                    point.setAsDouble(random.nextDouble());
                } else {
                    point.setAsInt(random.nextLong());
                }
                gauge.addDataPoints(point);
            }
            scope.addMetrics(Metric.newBuilder().setName("metric_" + metric).setUnit("1").setGauge(gauge));
        }
        Resource resource = Resource.newBuilder()
            .addAttributes(stringAttribute("service.name", "benchmark"))
            .addAttributes(stringAttribute("host.name", "host-" + random.nextInt(ATTRIBUTE_CARDINALITY)))
            .build();
        return ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(ResourceMetrics.newBuilder().setResource(resource).addScopeMetrics(scope))
            .build();
    }

    private static KeyValue stringAttribute(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value)).build();
    }
}
