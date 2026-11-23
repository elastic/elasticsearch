/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.core.Nullable;

/**
 * Detects Lucene's spatial queries over indexed points and shapes, whose scorers walk the BKD tree and collect the matches of
 * the whole leaf when they are built. The classes are package-private in {@code org.apache.lucene.document}, so they are matched
 * by name. Their doc-values siblings ({@code LatLonDocValuesQuery}, {@code LatLonShapeDocValuesQuery} and so on) and the
 * {@code LatLonPointDistanceFeatureQuery} scoring query are not matched.
 */
public final class IndexedSpatialQueries {

    /** How an indexed spatial query collects its matches on a leaf. */
    public enum Kind {
        /** {@code XYPointInGeometryQuery}: into a {@code DocIdSetBuilder}. */
        POINT_IN_GEOMETRY,
        /**
         * {@code LatLonPointDistanceQuery}: into a {@code DocIdSetBuilder}, or a bit set of the non-matching documents when
         * it matches most of a single-valued leaf.
         */
        POINT_DISTANCE,
        /**
         * Subclasses of {@code SpatialQuery} over points or triangles ({@code LatLonPointQuery}, {@code LatLonShapeQuery},
         * {@code LatLonShapeBoundingBoxQuery}, {@code XYShapeQuery}): into one or two bit sets or a {@code DocIdSetBuilder},
         * depending on the relation.
         */
        SPATIAL
    }

    static final String XY_POINT_IN_GEOMETRY_QUERY = "org.apache.lucene.document.XYPointInGeometryQuery";
    static final String LAT_LON_POINT_DISTANCE_QUERY = "org.apache.lucene.document.LatLonPointDistanceQuery";
    static final String SPATIAL_QUERY = "org.apache.lucene.document.SpatialQuery";
    /** The {@code SpatialQuery} subclass over doc values, which allocates nothing per leaf. */
    static final String SHAPE_DOC_VALUES_QUERY = "org.apache.lucene.document.BaseShapeDocValuesQuery";

    private IndexedSpatialQueries() {}

    /** Whether {@code query} is an indexed spatial query. */
    public static boolean isIndexedSpatialQuery(Query query) {
        return kindOf(query) != null;
    }

    /**
     * Whether {@code query} has an indexed spatial query anywhere in its tree, including under {@code MUST_NOT}: a negated clause
     * still builds its scorer. The walk sees through {@code BooleanQuery}, {@code ConstantScoreQuery} and
     * {@code IndexOrDocValuesQuery}, which visits both of its queries.
     */
    public static boolean containsIndexedSpatialQuery(Query query) {
        final boolean[] found = { false };
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(Query leaf) {
                if (isIndexedSpatialQuery(leaf)) {
                    found[0] = true;
                }
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;
            }
        });
        return found[0];
    }

    /** @return which indexed spatial query {@code query} is, or {@code null} if it is not one */
    @Nullable
    public static Kind kindOf(Query query) {
        return switch (query.getClass().getName()) {
            case XY_POINT_IN_GEOMETRY_QUERY -> Kind.POINT_IN_GEOMETRY;
            case LAT_LON_POINT_DISTANCE_QUERY -> Kind.POINT_DISTANCE;
            default -> isIndexedSpatialQuerySubclass(query.getClass()) ? Kind.SPATIAL : null;
        };
    }

    /** Whether {@code clazz} reaches {@code SpatialQuery} going up from it without passing the doc-values subclass. */
    private static boolean isIndexedSpatialQuerySubclass(Class<?> clazz) {
        for (Class<?> c = clazz.getSuperclass(); c != null && c != Query.class; c = c.getSuperclass()) {
            switch (c.getName()) {
                case SPATIAL_QUERY:
                    return true;
                case SHAPE_DOC_VALUES_QUERY:
                    return false;
                default:
                    break;
            }
        }
        return false;
    }
}
