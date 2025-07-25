/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.eson;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.ESONSource;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class ESONDeserializationBenchmark {

    private static final BytesRef BYTES_REF = new BytesRef(new byte[16384]);

    private BytesReference source;
    private BytesReference cborSource;
    private Map<String, Object> map;
    private ESONSource.ESONObject esonObject;

    private final Recycler<BytesRef> refRecycler = new Recycler<>() {
        @Override
        public V<BytesRef> obtain() {
            return new V<>() {
                @Override
                public BytesRef v() {
                    return BYTES_REF;
                }

                @Override
                public boolean isRecycled() {
                    return true;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public int pageSize() {
            return BYTES_REF.length;
        }
    };

    @Setup
    public void initResults() throws IOException {
        source = new BytesArray(
            "{\"@timestamp\":\"2021-04-28T19:45:28.222Z\",\"kubernetes\":{\"namespace\":\"namespace0\",\"node\":{\"name\":\"gke-apps-node-name-0\"},\"pod\":{\"name\":\"pod-name-pod-name-0\"},\"volume\":{\"name\":\"volume-0\",\"fs\":{\"capacity\":{\"bytes\":7883960320},\"used\":{\"bytes\":12288},\"inodes\":{\"used\":9,\"free\":1924786,\"count\":1924795},\"available\":{\"bytes\":7883948032}}}},\"metricset\":{\"name\":\"volume\",\"period\":10000},\"fields\":{\"cluster\":\"elastic-apps\"},\"host\":{\"name\":\"gke-apps-host-name0\"},\"agent\":{\"id\":\"96db921d-d0a0-4d00-93b7-2b6cfc591bc3\",\"version\":\"7.6.2\",\"type\":\"metricbeat\",\"ephemeral_id\":\"c0aee896-0c67-45e4-ba76-68fcd6ec4cde\",\"hostname\":\"gke-apps-host-name-0\"},\"ecs\":{\"version\":\"1.4.0\"},\"service\":{\"address\":\"service-address-0\",\"type\":\"kubernetes\"},\"event\":{\"dataset\":\"kubernetes.volume\",\"module\":\"kubernetes\",\"duration\":132588484}}"
        );
        XContentBuilder builder = XContentFactory.contentBuilder(CborXContent.cborXContent.type());
        map = XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
        builder.map(map, true);
        BytesRef bytesRef = BytesReference.bytes(builder).toBytesRef();
        cborSource = new BytesArray(bytesRef.bytes, bytesRef.offset, bytesRef.length);

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                source.array(),
                source.arrayOffset(),
                source.length()
            )
        ) {
            esonObject = new ESONSource.Builder().parse(parser);
        }
    }

    @Benchmark
    public void readCborMap(Blackhole bh) throws IOException {
        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(cborSource, false, XContentType.CBOR);
        Map<String, Object> obj = tuple.v2();
        bh.consume(obj);
    }

    @Benchmark
    public void writeJSONFromMap(Blackhole bh) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(JsonXContent.jsonXContent.type());
        builder.map(map, true);
        BytesReference bytes = BytesReference.bytes(builder);
        bh.consume(bytes);
    }

    @Benchmark
    public void writeJSONFromESON(Blackhole bh) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(JsonXContent.jsonXContent.type());
        esonObject.toXContent(builder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);
        bh.consume(bytes);
    }

    @Benchmark
    public void readMap(Blackhole bh) throws IOException {
        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(source, false, XContentType.JSON);
        Map<String, Object> obj = tuple.v2();
        bh.consume(obj);
    }

    @Benchmark
    public void readESON(Blackhole bh) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                source.array(),
                source.arrayOffset(),
                source.length()
            )
        ) {
            ESONSource.ESONObject eson = new ESONSource.Builder(refRecycler).parse(parser);
            bh.consume(eson);
        }
    }
}
