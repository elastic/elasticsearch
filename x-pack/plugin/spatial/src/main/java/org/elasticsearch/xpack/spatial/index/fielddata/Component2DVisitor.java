/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.PointValues;

/**
 * A {@link TriangleTreeReader.Visitor} implementation for {@link Component2D} geometries.
 * It can solve spatial relationships against a serialize triangle tree.
 */
public abstract class Component2DVisitor extends TriangleTreeReader.DecodedVisitor {

    protected final Component2D component2D;

    private Component2DVisitor(Component2D component2D, CoordinateEncoder encoder) {
        super(encoder);
        this.component2D = component2D;
    }

    /** If the relationship has been honour. */
    public abstract boolean matches();

    /** Reset the visitor to the initial state. */
    public abstract void reset();

    @Override
    public boolean pushDecodedX(double minX) {
        return component2D.getMaxX() >= minX;
    }

    @Override
    public boolean pushDecodedY(double minY) {
        return component2D.getMaxY() >= minY;
    }

    @Override
    public boolean pushDecoded(double maxX, double maxY) {
        return component2D.getMinX() <= maxX && component2D.getMinY() <= maxY;

    }

    @Override
    public boolean pushDecoded(double minX, double minY, double maxX, double maxY) {
        return pushDecoded(component2D.relate(minX, maxX, minY, maxY));
    }

    /** Relation between the query shape and the doc value bounding box. Depending on the query relationship,
     * decide if we should traverse the tree.
     *
     * @return if true, the visitor keeps traversing the tree, else it stops.
     * */
    abstract boolean pushDecoded(PointValues.Relation relation);

    /**
     * Creates a visitor from the provided Component2D and spatial relationship. Visitors are re-usable by
     * calling the {@link #reset()} method.
     */
    public static Component2DVisitor getVisitor(Component2D component2D, ShapeField.QueryRelation relation, CoordinateEncoder encoder) {
        return switch (relation) {
            case CONTAINS -> new ContainsVisitor(component2D, encoder);
            case INTERSECTS -> new IntersectsVisitor(component2D, encoder);
            case DISJOINT -> new DisjointVisitor(component2D, encoder);
            case WITHIN -> new WithinVisitor(component2D, encoder);
        };
    }

    /**
     * Intersects visitor stops as soon as there is one triangle intersecting the component
     */
    private static class IntersectsVisitor extends Component2DVisitor {

        boolean intersects;

        private IntersectsVisitor(Component2D component2D, CoordinateEncoder encoder) {
            super(component2D, encoder);
        }

        @Override
        public boolean matches() {
            return intersects;
        }

        @Override
        public void reset() {
            // Start assuming that shapes are disjoint. As soon an intersecting component is found,
            // stop traversing the tree.
            intersects = false;
        }

        @Override
        void visitDecodedPoint(double x, double y) {
            intersects = component2D.contains(x, y);
        }

        @Override
        void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
            intersects = component2D.intersectsLine(aX, aY, bX, bY);
        }

        @Override
        void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            intersects = component2D.intersectsTriangle(aX, aY, bX, bY, cX, cY);
        }

        @Override
        public boolean push() {
            // as far as shapes don't intersect, keep traversing the tree
            return intersects == false;
        }

        @Override
        boolean pushDecoded(PointValues.Relation relation) {
            if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                // shapes are disjoint, stop traversing the tree.
                return false;
            } else if (relation == PointValues.Relation.CELL_INSIDE_QUERY) {
                // shapes intersects, stop traversing the tree.
                intersects = true;
                return false;
            } else {
                // traverse the tree.
                return true;
            }
        }
    }

    /**
     * Disjoint visitor stops as soon as there is one triangle intersecting the component
     */
    private static class DisjointVisitor extends Component2DVisitor {

        boolean disjoint;

        private DisjointVisitor(Component2D component2D, CoordinateEncoder encoder) {
            super(component2D, encoder);
            disjoint = true;
        }

        @Override
        public boolean matches() {
            return disjoint;
        }

        @Override
        public void reset() {
            // Start assuming that shapes are disjoint. As soon an intersecting component is found,
            // stop traversing the tree.
            disjoint = true;
        }

        @Override
        void visitDecodedPoint(double x, double y) {
            disjoint = component2D.contains(x, y) == false;
        }

        @Override
        void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
            disjoint = component2D.intersectsLine(aX, aY, bX, bY) == false;
        }

        @Override
        void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            disjoint = component2D.intersectsTriangle(aX, aY, bX, bY, cX, cY) == false;
        }

        @Override
        public boolean push() {
            // as far as the shapes are disjoint, keep traversing the tree
            return disjoint;
        }

        @Override
        boolean pushDecoded(PointValues.Relation relation) {
            if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                // shapes are disjoint, stop traversing the tree.
                return false;
            } else if (relation == PointValues.Relation.CELL_INSIDE_QUERY) {
                // shapes intersects, stop traversing the tree.
                disjoint = false;
                return false;
            } else {
                // trasverse the tree
                return true;
            }
        }
    }

    /**
     * within visitor stops as soon as there is one triangle that is not within the component
     */
    private static class WithinVisitor extends Component2DVisitor {

        boolean within;

        private WithinVisitor(Component2D component2D, CoordinateEncoder encoder) {
            super(component2D, encoder);
            within = true;
        }

        @Override
        public boolean matches() {
            return within;
        }

        @Override
        public void reset() {
            // Start assuming that the doc value is within the query shape. As soon
            // as a component is not within the query, stop traversing the tree.
            within = true;
        }

        @Override
        void visitDecodedPoint(double x, double y) {
            within = component2D.contains(x, y);
        }

        @Override
        void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
            within = component2D.containsLine(aX, aY, bX, bY);
        }

        @Override
        void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            within = component2D.containsTriangle(aX, aY, bX, bY, cX, cY);
        }

        @Override
        public boolean push() {
            // as far as the doc value is within the query shape, keep traversing the tree
            return within;
        }

        @Override
        public boolean pushX(int minX) {
            // if any part of the tree is skipped, then the doc value is not within the shape,
            // stop traversing the tree
            within = super.pushX(minX);
            return within;
        }

        @Override
        public boolean pushY(int minY) {
            // if any part of the tree is skipped, then the doc value is not within the shape,
            // stop traversing the tree
            within = super.pushY(minY);
            return within;
        }

        @Override
        public boolean push(int maxX, int maxY) {
            // if any part of the tree is skipped, then the doc value is not within the shape,
            // stop traversing the tree
            within = super.push(maxX, maxY);
            return within;
        }

        @Override
        boolean pushDecoded(PointValues.Relation relation) {
            if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY) {
                // shapes are disjoint, stop traversing the tree.
                within = false;
            }
            return within;
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
            // Start assuming that shapes are disjoint. As soon
            // as a component has a NOTWITHIN relationship, stop traversing the tree.
            answer = Component2D.WithinRelation.DISJOINT;
        }

        @Override
        void visitDecodedPoint(double x, double y) {
            final Component2D.WithinRelation rel = component2D.withinPoint(x, y);
            if (rel != Component2D.WithinRelation.DISJOINT) {
                // Only override relationship if different to DISJOINT
                answer = rel;
            }
        }

        @Override
        void visitDecodedLine(double aX, double aY, double bX, double bY, byte metadata) {
            final boolean ab = (metadata & 1 << 4) == 1 << 4;
            final Component2D.WithinRelation rel = component2D.withinLine(aX, aY, ab, bX, bY);
            if (rel != Component2D.WithinRelation.DISJOINT) {
                // Only override relationship if different to DISJOINT
                answer = rel;
            }
        }

        @Override
        void visitDecodedTriangle(double aX, double aY, double bX, double bY, double cX, double cY, byte metadata) {
            final boolean ab = (metadata & 1 << 4) == 1 << 4;
            final boolean bc = (metadata & 1 << 5) == 1 << 5;
            final boolean ca = (metadata & 1 << 6) == 1 << 6;
            final Component2D.WithinRelation rel = component2D.withinTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
            if (rel != Component2D.WithinRelation.DISJOINT) {
                // Only override relationship if different to DISJOINT
                answer = rel;
            }
        }

        @Override
        public boolean push() {
            // If the relationship is NOTWITHIN, stop traversing the tree
            return answer != Component2D.WithinRelation.NOTWITHIN;
        }

        @Override
        boolean pushDecoded(PointValues.Relation relation) {
            // Only traverse the tree if the shapes intersects.
            return relation == PointValues.Relation.CELL_CROSSES_QUERY;
        }
    }
}
