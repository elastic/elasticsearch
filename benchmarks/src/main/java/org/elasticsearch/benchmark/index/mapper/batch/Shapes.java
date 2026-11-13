/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper.batch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;

/**
 * Catalog of named benchmark inputs: a mapping, a document generator and the indexing path the shape is expected to take.
 * Shapes are named entries rather than orthogonal JMH parameters because most combinations of type, value distribution and
 * sparsity are invalid or fall back under columnar mode, and a declared {@link Expect} lets setup prove each shape takes the
 * path it is measured on.
 */
public final class Shapes {

    private static final String TIMESTAMP = "@timestamp";

    public static final Map<String, BenchShape> CATALOG = buildCatalog();

    private Shapes() {}

    public static BenchShape get(String name) {
        BenchShape shape = CATALOG.get(name);
        if (shape == null) {
            throw new IllegalArgumentException("unknown shape [" + name + "], known shapes " + names());
        }
        return shape;
    }

    public static List<BenchShape> where(Predicate<BenchShape> predicate) {
        return CATALOG.values().stream().filter(predicate).toList();
    }

    public static List<String> names() {
        return List.copyOf(CATALOG.keySet());
    }

    public static NumericShapeBuilder numeric(String type) {
        return new NumericShapeBuilder(type);
    }

    private static Map<String, BenchShape> buildCatalog() {
        Map<String, BenchShape> shapes = new LinkedHashMap<>();
        for (NumberType type : List.of(
            NumberType.LONG,
            NumberType.INTEGER,
            NumberType.SHORT,
            NumberType.BYTE,
            NumberType.DOUBLE,
            NumberType.FLOAT,
            NumberType.HALF_FLOAT
        )) {
            ValueDist small = type.floating ? ValueDist.FRACTIONAL : ValueDist.SMALL_INTS;
            JsonKind kind = type.floating ? JsonKind.DOUBLE : JsonKind.LONG;
            add(shapes, numeric(type.typeName).values(small, kind).fields(4).build(type.typeName + ".dense.small"));
            add(shapes, numeric(type.typeName).values(ValueDist.RANDOM_FULL, kind).fields(4).build(type.typeName + ".dense.full"));
            add(shapes, numeric(type.typeName).values(small, kind).fields(4).absentEvery(10).build(type.typeName + ".absent10"));
            add(
                shapes,
                numeric(type.typeName).values(small, kind)
                    .fields(4)
                    .multi(1, 4)
                    .expect(type.floating ? Expect.FALLBACK_MAP : Expect.COLUMN)
                    .build(type.typeName + ".mv1to4")
            );
        }
        add(shapes, numeric("long").values(ValueDist.SMALL_INTS, JsonKind.STRING).fields(4).build("long.str"));
        add(shapes, numeric("long").values(ValueDist.SMALL_INTS, JsonKind.DOUBLE).fields(4).build("long.dbl"));
        add(shapes, numeric("integer").param("index", false).fields(4).build("integer.noindex"));
        add(shapes, numeric("long").fields(4).nullEvery(10).expect(Expect.FALLBACK_MAP).build("long.nullexplicit"));
        add(
            shapes,
            numeric("scaled_float").param("scaling_factor", 100)
                .values(ValueDist.FRACTIONAL, JsonKind.DOUBLE)
                .fields(4)
                .expect(Expect.FALLBACK_RESOLVE)
                .build("scaled_float.dense")
        );
        add(shapes, metrics4());
        return Collections.unmodifiableMap(shapes);
    }

    private static void add(Map<String, BenchShape> shapes, BenchShape shape) {
        if (shapes.putIfAbsent(shape.name(), shape) != null) {
            throw new IllegalStateException("duplicate shape [" + shape.name() + "]");
        }
    }

    private static BenchShape metrics4() {
        String[] names = { "bytes_in", "bytes_out", "latency" };
        return new BenchShape(
            "metrics4",
            IndexMode.COLUMNAR,
            mapping(names, "long", Map.of()),
            new Metrics4Docs(),
            EnumSet.of(Feature.DENSE, Feature.POINTS),
            Expect.COLUMN,
            List.of()
        );
    }

    private static String mapping(String[] names, String type, Map<String, Object> params) {
        try {
            XContentBuilder b = JsonXContent.contentBuilder();
            b.startObject().startObject("_doc").field("dynamic", false).startObject("properties");
            b.startObject(TIMESTAMP).field("type", "date").endObject();
            for (String name : names) {
                b.startObject(name).field("type", type);
                for (Map.Entry<String, Object> param : params.entrySet()) {
                    b.field(param.getKey(), param.getValue());
                }
                b.endObject();
            }
            b.endObject().endObject().endObject();
            return Strings.toString(b);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isFalse(Object value) {
        return value != null && "false".equals(value.toString());
    }

    public record BenchShape(
        String name,
        IndexMode mode,
        String mapping,
        DocGenerator docs,
        EnumSet<Feature> features,
        Expect expect,
        List<MapperPlugin> plugins
    ) {

        public boolean has(Feature f) {
            return features.contains(f);
        }
    }

    public interface DocGenerator {

        JsonKind kind();

        void write(XContentBuilder b, Random r, int doc) throws IOException;
    }

    public enum Expect {
        COLUMN,
        FALLBACK_RESOLVE,
        FALLBACK_MAP
    }

    public enum Feature {
        DENSE,
        ABSENT,
        EXPLICIT_NULL,
        MULTI_VALUE,
        OFFSETS,
        STRING_INPUT,
        DOUBLE_INPUT,
        NOT_INDEXED,
        POINTS
    }

    public enum JsonKind {
        LONG,
        DOUBLE,
        STRING
    }

    public enum ValueDist {
        SMALL_INTS {
            @Override
            public long nextLong(Random r, int doc) {
                return r.nextInt(128);
            }
        },
        MONOTONIC_TS {
            @Override
            public long nextLong(Random r, int doc) {
                return TS_BASE + doc * TS_STEP + r.nextInt((int) TS_STEP);
            }
        },
        RANDOM_FULL {
            @Override
            public long nextLong(Random r, int doc) {
                return r.nextLong();
            }

            @Override
            public double nextDouble(Random r, int doc) {
                while (true) {
                    double d = Double.longBitsToDouble(r.nextLong());
                    if (Double.isFinite(d)) {
                        return d;
                    }
                }
            }
        },
        FRACTIONAL {
            @Override
            public long nextLong(Random r, int doc) {
                return Math.round(nextDouble(r, doc));
            }

            @Override
            public double nextDouble(Random r, int doc) {
                return 50.0 + (r.nextInt(201) - 100) * 0.01;
            }
        };

        private static final long TS_BASE = 1_704_067_200_000L;
        private static final long TS_STEP = 500L;

        public abstract long nextLong(Random r, int doc);

        public double nextDouble(Random r, int doc) {
            return nextLong(r, doc);
        }
    }

    private enum NumberType {
        LONG("long", false),
        INTEGER("integer", false),
        SHORT("short", false),
        BYTE("byte", false),
        DOUBLE("double", true),
        FLOAT("float", true),
        HALF_FLOAT("half_float", true),
        SCALED_FLOAT("scaled_float", true);

        private static final double HALF_FLOAT_MAX = 65504.0;
        private static final int HALF_FLOAT_MIN_EXPONENT = -14;
        private static final int HALF_FLOAT_MAX_EXPONENT = 15;

        private final String typeName;
        private final boolean floating;

        NumberType(String typeName, boolean floating) {
            this.typeName = typeName;
            this.floating = floating;
        }

        static NumberType of(String typeName) {
            for (NumberType type : values()) {
                if (type.typeName.equals(typeName)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("unsupported numeric type [" + typeName + "]");
        }

        long narrow(long v) {
            return switch (this) {
                case INTEGER -> (int) v;
                case SHORT -> (short) v;
                case BYTE -> (byte) v;
                case LONG -> v;
                default -> throw new IllegalStateException("unexpected integral type [" + this + "]");
            };
        }

        double narrow(double v) {
            return switch (this) {
                case FLOAT -> wrapExponent(v, Float.MIN_EXPONENT, Float.MAX_EXPONENT, Float.MAX_VALUE);
                case HALF_FLOAT -> wrapExponent(v, HALF_FLOAT_MIN_EXPONENT, HALF_FLOAT_MAX_EXPONENT, HALF_FLOAT_MAX);
                case DOUBLE, SCALED_FLOAT -> v;
                default -> throw new IllegalStateException("unexpected floating type [" + this + "]");
            };
        }

        boolean singlePrecision() {
            return switch (this) {
                case FLOAT, HALF_FLOAT -> true;
                case LONG, INTEGER, SHORT, BYTE, DOUBLE, SCALED_FLOAT -> false;
                default -> throw new IllegalStateException("unexpected numeric type [" + this + "]");
            };
        }

        private static double wrapExponent(double v, int minExponent, int maxExponent, double max) {
            if (v == 0) {
                return v;
            }
            int exponent = Math.getExponent(v);
            if (exponent < minExponent || exponent > maxExponent) {
                int wrapped = Math.floorMod(exponent - minExponent, maxExponent - minExponent + 1) + minExponent;
                v = Math.scalb(v, wrapped - exponent);
            }
            return Math.max(-max, Math.min(max, v));
        }
    }

    private record NumericDocs(
        NumberType type,
        ValueDist dist,
        JsonKind kind,
        String[] names,
        int absentEvery,
        int nullEvery,
        int multiMin,
        int multiMax
    ) implements DocGenerator {

        @Override
        public void write(XContentBuilder b, Random r, int doc) throws IOException {
            b.field(TIMESTAMP, ValueDist.MONOTONIC_TS.nextLong(r, doc));
            for (int f = 0; f < names.length; f++) {
                if (absentEvery > 0 && (doc + f + 1) % absentEvery == 0) {
                    continue;
                }
                b.field(names[f]);
                if (nullEvery > 0 && (doc + f + 1) % nullEvery == 0) {
                    b.nullValue();
                } else if (multiMax > 0) {
                    int count = multiMin + r.nextInt(multiMax - multiMin + 1);
                    b.startArray();
                    for (int i = 0; i < count; i++) {
                        writeValue(b, r, doc);
                    }
                    b.endArray();
                } else {
                    writeValue(b, r, doc);
                }
            }
        }

        private void writeValue(XContentBuilder b, Random r, int doc) throws IOException {
            switch (kind) {
                case LONG -> b.value(longValue(r, doc));
                case DOUBLE -> {
                    double v = doubleValue(r, doc);
                    if (type.singlePrecision()) {
                        b.value((float) v);
                    } else {
                        b.value(v);
                    }
                }
                case STRING -> b.value(type.floating ? Double.toString(doubleValue(r, doc)) : Long.toString(longValue(r, doc)));
            }
        }

        private long longValue(Random r, int doc) {
            return type.narrow(dist.nextLong(r, doc));
        }

        private double doubleValue(Random r, int doc) {
            return type.floating ? type.narrow(dist.nextDouble(r, doc)) : longValue(r, doc);
        }
    }

    private record Metrics4Docs() implements DocGenerator {

        @Override
        public JsonKind kind() {
            return JsonKind.LONG;
        }

        @Override
        public void write(XContentBuilder b, Random r, int doc) throws IOException {
            b.field(TIMESTAMP, ValueDist.MONOTONIC_TS.nextLong(r, doc));
            b.field("bytes_in", r.nextInt(10_000_000));
            b.field("bytes_out", r.nextInt(10_000_000));
            b.field("latency", r.nextInt(100_000));
        }
    }

    public static final class NumericShapeBuilder {
        private final NumberType type;
        private final Map<String, Object> params = new LinkedHashMap<>();
        private ValueDist dist;
        private JsonKind kind;
        private int absentEvery;
        private int nullEvery;
        private int multiMin;
        private int multiMax;
        private int fields = 1;
        private Expect expect = Expect.COLUMN;

        private NumericShapeBuilder(String type) {
            this.type = NumberType.of(type);
            this.dist = this.type.floating ? ValueDist.FRACTIONAL : ValueDist.SMALL_INTS;
            this.kind = this.type.floating ? JsonKind.DOUBLE : JsonKind.LONG;
        }

        public NumericShapeBuilder param(String key, Object value) {
            params.put(key, value);
            return this;
        }

        public NumericShapeBuilder values(ValueDist dist, JsonKind kind) {
            this.dist = dist;
            this.kind = kind;
            return this;
        }

        public NumericShapeBuilder absentEvery(int n) {
            this.absentEvery = requirePositive("absentEvery", n);
            return this;
        }

        public NumericShapeBuilder nullEvery(int n) {
            this.nullEvery = requirePositive("nullEvery", n);
            return this;
        }

        public NumericShapeBuilder multi(int min, int max) {
            if (min < 1 || max < min) {
                throw new IllegalArgumentException("multi requires 1 <= min <= max but got [" + min + ", " + max + "]");
            }
            this.multiMin = min;
            this.multiMax = max;
            return this;
        }

        public NumericShapeBuilder fields(int n) {
            this.fields = requirePositive("fields", n);
            return this;
        }

        public NumericShapeBuilder expect(Expect expect) {
            this.expect = expect;
            return this;
        }

        public BenchShape build(String name) {
            String[] names = new String[fields];
            for (int i = 0; i < fields; i++) {
                names[i] = "f" + i;
            }
            return new BenchShape(
                name,
                IndexMode.COLUMNAR,
                mapping(names, type.typeName, params),
                new NumericDocs(type, dist, kind, names, absentEvery, nullEvery, multiMin, multiMax),
                features(),
                expect,
                plugins()
            );
        }

        private EnumSet<Feature> features() {
            EnumSet<Feature> features = EnumSet.noneOf(Feature.class);
            if (absentEvery == 0 && nullEvery == 0 && multiMax == 0) {
                features.add(Feature.DENSE);
            }
            if (absentEvery > 0) {
                features.add(Feature.ABSENT);
            }
            if (nullEvery > 0) {
                features.add(Feature.EXPLICIT_NULL);
            }
            if (multiMax > 0) {
                features.add(Feature.MULTI_VALUE);
                features.add(Feature.OFFSETS);
            }
            switch (kind) {
                case STRING -> features.add(Feature.STRING_INPUT);
                case DOUBLE -> features.add(Feature.DOUBLE_INPUT);
                case LONG -> {
                }
                default -> throw new IllegalStateException("unexpected json kind [" + kind + "]");
            }
            if (isFalse(params.get("index"))) {
                features.add(Feature.NOT_INDEXED);
            } else {
                features.add(Feature.POINTS);
            }
            return features;
        }

        private List<MapperPlugin> plugins() {
            return type == NumberType.SCALED_FLOAT ? List.of(new MapperExtrasPlugin()) : List.of();
        }

        private static int requirePositive(String what, int n) {
            if (n < 1) {
                throw new IllegalArgumentException(what + " must be positive but got [" + n + "]");
            }
            return n;
        }
    }
}
