/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeWriter.AB_FROM_TRIANGLE;
import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeWriter.BC_FROM_TRIANGLE;
import static org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeWriter.CA_FROM_TRIANGLE;

/** Visitor for triangle interval tree.
 *
 * @see TriangleTreeReader
 * */
public interface TriangleTreeVisitor {

    /** visit a node point. */
    void visitPoint(int x, int y);

    /** visit a node line. */
    void visitLine(int aX, int aY, int bX, int bY, byte metadata);

    /** visit a node triangle. */
    void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata);

    /** Should the visitor keep visiting the tree. Called after visiting a node or skipping
     * a tree branch, if the return value is {@code false}, no more nodes will be visited. */
    boolean push();

    /** Should the visitor visit nodes that have bounds greater or equal
     * than the {@code minX} provided. */
    boolean pushX(int minX);

    /** Should the visitor visit nodes that have bounds greater or equal
     * than the {@code minY} provided. */
    boolean pushY(int minY);

    /** Should the visitor visit nodes that have bounds lower or equal than the
     * {@code maxX} and {@code minX} provided. */
    boolean push(int maxX, int maxY);

    /** Should the visitor visit the tree given the bounding box of the tree. Called before
     * visiting the tree. */
    boolean push(int minX, int minY, int maxX, int maxY);

    static boolean abFromTriangle(byte metadata) {
        return (metadata & AB_FROM_TRIANGLE) == AB_FROM_TRIANGLE;
    }

    static boolean bcFromTriangle(byte metadata) {
        return (metadata & BC_FROM_TRIANGLE) == BC_FROM_TRIANGLE;
    }

    static boolean caFromTriangle(byte metadata) {
        return (metadata & CA_FROM_TRIANGLE) == CA_FROM_TRIANGLE;
    }

    /** Visitor for triangle interval tree which decodes the coordinates */
    abstract class TriangleTreeDecodedVisitor implements TriangleTreeVisitor {

        private final CoordinateEncoder encoder;

        protected TriangleTreeDecodedVisitor(CoordinateEncoder encoder) {
            this.encoder = encoder;
        }

        @Override
        public final void visitPoint(int x, int y) {
            visitDecodedPoint(encoder.decodeX(x), encoder.decodeY(y));
        }

        /**
         * Equivalent to {@link #visitPoint(int, int)} but coordinates are decoded.
         */
        protected abstract void visitDecodedPoint(double x, double y);

        @Override
        public final void visitLine(int aX, int aY, int bX, int bY, byte metadata) {
            visitDecodedLine(encoder.decodeX(aX), encoder.decodeY(aY), encoder.decodeX(bX), encoder.decodeY(bY), metadata);
        }

        /**
         * Equivalent to {@link #visitLine(int, int, int, int, byte)} but coordinates are decoded.
         */
        protected abstract void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata);

        @Override
        public final void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {
            visitDecodedTriangle(
                encoder.decodeX(aX),
                encoder.decodeY(aY),
                encoder.decodeX(bX),
                encoder.decodeY(bY),
                encoder.decodeX(cX),
                encoder.decodeY(cY),
                metadata
            );
        }

        /**
         * Equivalent to {@link #visitTriangle(int, int, int, int, int, int, byte)} but coordinates are decoded.
         */
        protected abstract void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata);

        @Override
        public final boolean pushX(int minX) {
            return pushDecodedX(encoder.decodeX(minX));
        }

        /**
         * Equivalent to {@link #pushX(int)}  but coordinates are decoded.
         */
        protected abstract boolean pushDecodedX(double minX);

        @Override
        public final boolean pushY(int minY) {
            return pushDecodedY(encoder.decodeY(minY));
        }

        /**
         * Equivalent to {@link #pushY(int)}  but coordinates are decoded.
         */
        protected abstract boolean pushDecodedY(double minX);

        @Override
        public final boolean push(int maxX, int maxY) {
            return pushDecoded(encoder.decodeX(maxX), encoder.decodeY(maxY));
        }

        /**
         * Equivalent to {@link #push(int, int)} but coordinates are decoded.
         */
        protected abstract boolean pushDecoded(double maxX, double maxY);

        @Override
        public final boolean push(int minX, int minY, int maxX, int maxY) {
            return pushDecoded(encoder.decodeX(minX), encoder.decodeY(minY), encoder.decodeX(maxX), encoder.decodeY(maxY));
        }

        /**
         * Equivalent to {@link #push(int, int, int, int)} but coordinates are decoded.
         */
        protected abstract boolean pushDecoded(double minX, double minY, double maxX, double maxY);
    }
}
