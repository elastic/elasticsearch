/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.xpack.spatial.index.fielddata.Component2DVisitor;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.GeometryDocValueReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Lucene geometry query for {@link org.elasticsearch.xpack.spatial.index.mapper.BinaryGeoShapeDocValuesField}. */
class LatLonShapeDocValuesQuery extends Query {

    private final String field;
    private final LatLonGeometry[] geometries;
    private final ShapeField.QueryRelation relation;

    LatLonShapeDocValuesQuery(String field, ShapeField.QueryRelation relation, LatLonGeometry... geometries) {
        if (field == null) {
            throw new IllegalArgumentException("field must not be null");
        }
        this.field = field;
        this.geometries = geometries;
        this.relation = relation;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        if (this.field.equals(field) == false) {
            sb.append(this.field);
            sb.append(':');
            sb.append(relation);
            sb.append(':');
        }
        sb.append(Arrays.toString(geometries));
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        LatLonShapeDocValuesQuery other = (LatLonShapeDocValuesQuery) obj;
        return field.equals(other.field) && relation == other.relation && Arrays.equals(geometries, other.geometries);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + field.hashCode();
        h = 31 * h + relation.hashCode();
        h = 31 * h + Arrays.hashCode(geometries);
        return h;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
         if (relation == ShapeField.QueryRelation.CONTAINS) {
             return getContainsWeight(scoreMode, boost);
         } else {
            return getStandardWeight(scoreMode, boost);
         }
    }

    private ConstantScoreWeight getStandardWeight(ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            final Component2D component2D = LatLonGeometry.create(geometries);

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(field);
                if (values == null) {
                    return null;
                }
                final GeometryDocValueReader reader = new GeometryDocValueReader();
                final Component2DVisitor visitor = Component2DVisitor.getVisitor(component2D, relation, CoordinateEncoder.GEO);

                final TwoPhaseIterator iterator = new TwoPhaseIterator(values) {

                    @Override
                    public boolean matches() throws IOException {
                        reader.reset(values.binaryValue());
                        visitor.reset();
                        reader.visit(visitor);
                        return visitor.matches();
                    }

                    @Override
                    public float matchCost() {
                        return 1000f; // TODO: what should it be?
                    }
                };
                return new ConstantScoreScorer(this, boost, scoreMode, iterator);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }

        };
    }

    private ConstantScoreWeight getContainsWeight(ScoreMode scoreMode, float boost) {
        final List<Component2D> components2D = new ArrayList<>(geometries.length);
        for (int i = 0; i < geometries.length; i++) {
            LatLonGeometry geometry = geometries[i];
            if (geometry instanceof Rectangle) {
                Rectangle r = (Rectangle) geometry;
                if (r.minLon > r.maxLon) {
                    components2D.add(LatLonGeometry.create(new Rectangle(r.minLat, r.maxLat, r.minLon, 180)));
                    components2D.add(LatLonGeometry.create(new Rectangle(r.minLat, r.maxLat, -180, r.maxLon)));
                    continue;
                }
            }
            components2D.add(LatLonGeometry.create(geometry));
        }
        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(field);
                if (values == null) {
                    return null;
                }
                final GeometryDocValueReader reader = new GeometryDocValueReader();
                final Component2DVisitor[] visitors = new Component2DVisitor[components2D.size()];
                for (int i = 0; i < components2D.size(); i++) {
                    visitors[i] = Component2DVisitor.getVisitor(components2D.get(i), relation, CoordinateEncoder.GEO);
                }

                final TwoPhaseIterator iterator = new TwoPhaseIterator(values) {

                    @Override
                    public boolean matches() throws IOException {
                        reader.reset(values.binaryValue());
                        for (Component2DVisitor visitor : visitors) {
                            visitor.reset();
                            reader.visit(visitor);
                            if (visitor.matches() == false) {
                                return false;
                            }
                        }
                        return true;
                    }

                    @Override
                    public float matchCost() {
                        return 1000f; // TODO: what should it be?
                    }
                };
                return new ConstantScoreScorer(this, boost, scoreMode, iterator);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, field);
            }
        };
    }
}
