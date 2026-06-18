/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * A functional interface representing a binary spatial geometry operation that combines two
 * geometries encoded as WKB ({@link BytesRef}) and returns the result as WKB.
 *
 * <p>Three implementations are provided:
 * <ul>
 *   <li>{@link JtsGeometryOperator} — decodes WKB to JTS, applies the operation (e.g.
 *       {@code Geometry::intersection}), and re-encodes the result to WKB</li>
 *   <li>{@link HybridGeometryOperator} — pre-checks disjointness via Lucene triangle-tree and
 *       returns a shortcut result (WKB) when disjoint, falling through to JTS otherwise</li>
 *   <li>{@link TriangleDecompositionOperator} — decomposes geometry A into triangles via the
 *       Lucene triangle-tree and accumulates per-triangle JTS results, returning WKB</li>
 * </ul>
 */
@FunctionalInterface
public interface GeometryOperator {

    /**
     * Apply the binary spatial operation to geometries encoded as WKB.
     *
     * @param wkbA WKB of the first geometry (left-hand argument)
     * @param wkbB WKB of the second geometry (right-hand argument)
     * @return WKB of the result geometry
     * @throws IOException if either input cannot be decoded or the operation fails
     */
    BytesRef apply(BytesRef wkbA, BytesRef wkbB) throws IOException;
}
