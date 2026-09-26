/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that ST_SIMPLIFY handles very large geometries without error, and that
 * ST_SIMPLIFYPRESERVETOPOLOGY converts a stack overflow on very complex geometries into
 * a descriptive {@link IllegalArgumentException} rather than crashing the node.
 *
 * <p>The {@code polygon.wkt.gz} resource contains a real-world polygon with ~49 000 vertices that
 * triggers {@code StackOverflowError} in JTS's recursive simplification algorithm
 * ({@code TaggedLineStringSimplifier}) under typical JVM stack-size settings.
 */
public class StSimplifyStackOverflowTests extends ESTestCase {

    /**
     * Verifies that simplifying a 49K-vertex polygon with ST_SIMPLIFY's iterative Douglas-Peucker
     * implementation completes successfully without any error. {@link IterativeDouglasPeuckerSimplifier}
     * uses a heap-allocated stack instead of JVM call-stack recursion, so it cannot cause
     * {@link StackOverflowError} regardless of geometry size or JVM stack depth.
     */
    public void testComplexPolygonDouglasPeuckerNoStackOverflow() throws IOException {
        BytesRef geometryBytes = loadPolygonWkb();
        SpatialGeometryBlockProcessor processor = new SpatialGeometryBlockProcessor(
            UNSPECIFIED,
            IterativeDouglasPeuckerSimplifier::simplify
        );
        BytesRef result = processor.processSingleGeometry(geometryBytes, 0.001);
        assertNotNull("expected a simplified geometry, not null", result);
    }

    /**
     * Verifies that {@link SpatialGeometryBlockProcessor} converts a {@link StackOverflowError}
     * from any operation into a user-facing {@link IllegalArgumentException} whose message names
     * the vertex count. ST_SIMPLIFYPRESERVETOPOLOGY uses JTS's recursive
     * {@code TopologyPreservingSimplifier}, which can exhaust the JVM call stack on very complex
     * geometries; this test uses a synthetic operation that always throws {@code StackOverflowError}
     * to test that conversion reliably, independent of JVM stack depth.
     */
    public void testStackOverflowConvertedToIllegalArgument() {
        SpatialGeometryBlockProcessor processor = new SpatialGeometryBlockProcessor(UNSPECIFIED, (geometry, param) -> {
            throw new StackOverflowError();
        });
        BytesRef geometryBytes = UNSPECIFIED.wktToWkb("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> processor.processSingleGeometry(geometryBytes, 0.001)
        );
        assertThat(e.getMessage(), containsString("vertices"));
    }

    private BytesRef loadPolygonWkb() throws IOException {
        InputStream raw = getClass().getResourceAsStream("polygon.wkt.gz");
        assertNotNull("polygon.wkt.gz test resource not found", raw);
        try (
            GZIPInputStream gz = new GZIPInputStream(raw);
            BufferedReader reader = new BufferedReader(new InputStreamReader(gz, StandardCharsets.UTF_8))
        ) {
            String wkt = reader.readLine();
            assertNotNull("polygon.wkt.gz was empty", wkt);
            return UNSPECIFIED.wktToWkb(wkt);
        }
    }
}
