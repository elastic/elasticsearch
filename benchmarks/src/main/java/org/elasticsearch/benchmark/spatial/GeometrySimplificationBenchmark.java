/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.spatial;

import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.simplify.GeometrySimplifier;
import org.elasticsearch.geometry.simplify.SimplificationErrorCalculator;
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class GeometrySimplificationBenchmark {
    @Param({ "cartesiantrianglearea", "triangleArea", "triangleheight", "heightandbackpathdistance" })
    public String calculatorName;

    @Param({ "10", "100", "1000", "10000", "20000" })
    public int maxPoints;

    private GeometrySimplifier<LinearRing> simplifier;
    private static LinearRing ring;

    @Setup
    public void setup() throws ParseException, IOException {
        SimplificationErrorCalculator calculator = SimplificationErrorCalculator.byName(calculatorName);
        this.simplifier = new GeometrySimplifier.LinearRingSimplifier(maxPoints, calculator);
        if (ring == null) {
            ring = loadRing("us.json.gz");
        }
    }

    @Benchmark
    public void simplify(Blackhole bh) {
        bh.consume(simplifier.simplify(ring));
    }

    private static LinearRing loadRing(@SuppressWarnings("SameParameterValue") String name) throws IOException, ParseException {
        String json = loadJsonFile(name);
        org.apache.lucene.geo.Polygon[] lucenePolygons = org.apache.lucene.geo.Polygon.fromGeoJSON(json);
        LinearRing ring = null;
        for (org.apache.lucene.geo.Polygon lucenePolygon : lucenePolygons) {
            double[] x = lucenePolygon.getPolyLons();
            double[] y = lucenePolygon.getPolyLats();
            if (ring == null || x.length > ring.length()) {
                ring = new LinearRing(x, y);
            }
        }
        return ring;
    }

    private static String loadJsonFile(String name) throws IOException {
        InputStream is = GeometrySimplificationBenchmark.class.getResourceAsStream(name);
        if (is == null) {
            throw new FileNotFoundException("classpath resource not found: " + name);
        }
        if (name.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder();
        reader.lines().forEach(builder::append);
        return builder.toString();
    }
}
