/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.PointValues;

/**
 * A {@link TriangleTreeReader.Visitor} implementation for {@link Component2D} geometries.
 * It can solve spatial relationships against a serialize triangle tree.
 */
public abstract class Component2DVisitor implements TriangleTreeReader.Visitor {

    protected final Component2D component2D;
    private final CoordinateEncoder encoder;

    private Component2DVisitor(Component2D component2D, CoordinateEncoder encoder) {
        this.component2D = component2D;
        this.encoder = encoder;
    }

    public abstract boolean matches();

    public abstract void reset();

    @Override
    public void visitPoint(int x, int y) {
        doVisitPoint(encoder.decodeX(x), encoder.decodeY(y));
    }

    abstract void doVisitPoint(double x, double y);

    @Override
    public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {
        doVisitLine(encoder.decodeX(aX), encoder.decodeY(aY), encoder.decodeX(bX), encoder.decodeY(bY), metadata);
    }

    abstract void doVisitLine(double aX, double aY, double bX, double bY, byte metadata);

    @Override
    public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {
        doVisitTriangle(
            encoder.decodeX(aX),
            encoder.decodeY(aY),
            encoder.decodeX(bX),
            encoder.decodeY(bY),
            encoder.decodeX(cX),
            encoder.decodeY(cY),
            metadata
        );
    }

    abstract void doVisitTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata);

    @Override
    public boolean pushX(int minX) {
        return component2D.getMaxX() >= encoder.decodeX(minX);
    }

    @Override
    public boolean pushY(int minY) {
        return component2D.getMaxY() >= encoder.decodeY(minY);
    }

    @Override
    public boolean push(int maxX, int maxY) {
        return component2D.getMinX() <= encoder.decodeX(maxX) &&
               component2D.getMinY() <= encoder.decodeY(maxY);

    }

    @Override
    public boolean push(int minX, int minY, int maxX, int maxY) {
        final PointValues.Relation relation = component2D.relate(
            encoder.decodeX(minX),
            encoder.decodeX(maxX),
            encoder.decodeY(minY),
            encoder.decodeY(maxY)
        );
        return doPush(relation);
    }

    abstract boolean doPush(PointValues.Relation relation);

    /**
     * Creates a visitor from the provided Component2D and spatial relationship. Visitors are re-usable by
     * calling the {@link #reset()} method.
     */
    public static Component2DVisitor getVisitor(
        Component2D component2D,
        ShapeField.QueryRelation relation,
        CoordinateEncoder encoder
    ) {
        switch (relation) {
            case CONTAINS:
                return new ContainsVisitor(component2D, encoder);
            case INTERSECTS:
                return new IntersectsVisitor(component2D, encoder);
            case DISJOINT:
                return new DisjointVisitor(component2D, encoder);
            case WITHIN:
                return new WithinVisitor(component2D, encoder);
            default:
                throw new IllegalArgumentException("Invalid query relation:[" + relation + "]");
        }
    }

    /**
     * Intersects visitor stops as soon as there is one triangle intersecting the component
     */
    private static class IntersectsVisitor extends Component2DVisitor {

        boolean answer;

        private IntersectsVisitor(Component2D component2D, CoordinateEncoder encoder) {
            super(component2D, encoder);
        }

        @Override
        public boolean matches() {
            return answer;
        }

        @Override
        public void reset() {
            answer = false;
        }

        @Override
        void doVisitPoint(double x, double y) {
            answer = component2D.contains(x, y);
        }

        @Override
        void doVisitLine(double aX, double aY, double bX, double bY, byte metadata) {
            answer = component2D.intersectsLine(aX, aY, bX, bY);
        }

        @Override
        void doVisitTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            answer = component2D.intersectsTriangle(aX, aY, bX, bY, cX, cY);
        }

        @Override
        public boolean push() {
            return answer == false;
        }

        @Override
       boolean doPush(PointValues.Relation relation) {
            if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                return false;
            } else if (relation == PointValues.Relation.CELL_INSIDE_QUERY) {
                answer = true;
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * Disjoint visitor stops as soon as there is one triangle intersecting the component
     */
    private static class DisjointVisitor extends Component2DVisitor {

        boolean answer;

        private DisjointVisitor(Component2D component2D, CoordinateEncoder encoder) {
            super(component2D, encoder);
            answer = true;
        }

        @Override
        public boolean matches() {
            return answer;
        }

        @Override
        public void reset() {
            answer = true;
        }


        @Override
        void doVisitPoint(double x, double y) {
            answer = component2D.contains(x, y) == false;
        }

        @Override
        void doVisitLine(double aX, double aY, double bX, double bY, byte metadata) {
            answer = component2D.intersectsLine(aX, aY, bX, bY) == false;
        }

        @Override
        void doVisitTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            answer = component2D.intersectsTriangle(aX, aY, bX, bY, cX, cY) == false;
        }

        @Override
        public boolean push() {
            return answer;
        }

        @Override
        boolean doPush(PointValues.Relation relation) {
            if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                return false;
            } else if (relation == PointValues.Relation.CELL_INSIDE_QUERY) {
                answer = false;
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * within visitor stops as soon as there is one triangle that is not within the component
     */
    private static class WithinVisitor extends Component2DVisitor {

        boolean answer;

        private WithinVisitor(Component2D component2D, CoordinateEncoder encoder) {
            super(component2D, encoder);
            answer = true;
        }

        @Override
        public boolean matches() {
            return answer;
        }

        @Override
        public void reset() {
            answer = true;
        }

        @Override
        void doVisitPoint(double x, double y) {
            answer = component2D.contains(x, y);
        }

        @Override
        void doVisitLine(double aX, double aY, double bX, double bY, byte metadata) {
            answer = component2D.containsLine(aX, aY, bX, bY);
        }

        @Override
        void doVisitTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            answer = component2D.containsTriangle(aX, aY, bX, bY, cX, cY);
        }

        @Override
        public boolean push() {
            return answer;
        }

        @Override
        public boolean pushX(int minX) {
            answer = super.pushX(minX);
            return answer;
        }

        @Override
        public boolean pushY(int minY) {
            answer = super.pushY(minY);
            return answer;
        }

        @Override
        public boolean push(int maxX, int maxY) {
            answer = super.push(maxX, maxY);
            return answer;
        }

        @Override
        boolean doPush(PointValues.Relation relation) {
            if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                answer = false;
            }
            return answer;
        }
    }

    /**
     * contains visitor stops as soon as there is one triangle that intersects the component
     * with an edge belonging to the original polygon.
     */
    private static class ContainsVisitor extends Component2DVisitor {

        Component2D.WithinRelation answer;

        private ContainsVisitor(Component2D component2D, CoordinateEncoder encoder) {
            super(component2D, encoder);
            answer = Component2D.WithinRelation.DISJOINT;
        }

        @Override
        public boolean matches() {
            return answer == Component2D.WithinRelation.CANDIDATE;
        }

        @Override
        public void reset() {
            answer = Component2D.WithinRelation.DISJOINT;
        }

        @Override
        void doVisitPoint(double x, double y) {
            final Component2D.WithinRelation rel = component2D.withinPoint(x, y);
            if (rel != Component2D.WithinRelation.DISJOINT) {
                answer = rel;
            }
        }

        @Override
        void doVisitLine(double aX, double aY, double bX, double bY, byte metadata) {
            final boolean ab = (metadata & 1 << 4) == 1 << 4;
            final Component2D.WithinRelation rel = component2D.withinLine(aX, aY, ab, bX, bY);
            if (rel != Component2D.WithinRelation.DISJOINT) {
                answer = rel;
            }
        }

        @Override
        void doVisitTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            final boolean ab = (metadata & 1 << 4) == 1 << 4;
            final boolean bc = (metadata & 1 << 5) == 1 << 5;
            final boolean ca = (metadata & 1 << 6) == 1 << 6;
            final Component2D.WithinRelation rel = component2D.withinTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
            if (rel != Component2D.WithinRelation.DISJOINT) {
                answer = rel;
            }
        }

        @Override
        public boolean push() {
            return answer != Component2D.WithinRelation.NOTWITHIN;
        }

        @Override
        boolean doPush(PointValues.Relation relation) {
            return relation == PointValues.Relation.CELL_CROSSES_QUERY;
        }
    }
}
