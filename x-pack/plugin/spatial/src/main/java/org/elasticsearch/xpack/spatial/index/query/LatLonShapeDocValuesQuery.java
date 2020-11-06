/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
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
import java.util.Arrays;

/** Lucene geometry query for {@link org.elasticsearch.xpack.spatial.index.mapper.BinaryGeoShapeDocValuesField}. */
class LatLonShapeDocValuesQuery extends Query {

    private final String field;
    private final LatLonGeometry[] geometry;
    private final ShapeField.QueryRelation relation;

    LatLonShapeDocValuesQuery(String field, ShapeField.QueryRelation relation, LatLonGeometry... geometry) {
        if (field == null) {
            throw new IllegalArgumentException("field must not be null");
        }
        this.field = field;
        this.geometry = geometry;
        this.relation = relation;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        if (!this.field.equals(field)) {
            sb.append(this.field);
            sb.append(':');
            sb.append(relation);
            sb.append(':');
        }
        sb.append(Arrays.toString(geometry));
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        LatLonShapeDocValuesQuery other = (LatLonShapeDocValuesQuery) obj;
        return field.equals(other.field) && relation == other.relation && Arrays.equals(geometry, other.geometry);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + field.hashCode();
        h = 31 * h + relation.hashCode();
        h = 31 * h + Arrays.hashCode(geometry);
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

        return new ConstantScoreWeight(this, boost) {
            final Component2D component2D = LatLonGeometry.create(geometry);

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
}
