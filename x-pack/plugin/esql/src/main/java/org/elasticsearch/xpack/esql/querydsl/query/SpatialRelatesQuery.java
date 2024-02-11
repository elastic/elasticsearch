/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.spatial.CartesianShapeDocValuesQuery;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;

public class SpatialRelatesQuery extends Query {
    private final String field;
    private final ShapeField.QueryRelation queryRelation;
    private final Geometry shape;
    private final DataType dataType;

    public SpatialRelatesQuery(Source source, String field, ShapeField.QueryRelation queryRelation, Geometry shape, DataType dataType) {
        super(source);
        this.field = field;
        this.queryRelation = queryRelation;
        this.shape = shape;
        this.dataType = dataType;
    }

    @Override
    public boolean containsNestedField(String path, String field) {
        return false;
    }

    @Override
    public Query addNestedField(String path, String field, String format, boolean hasDocValues) {
        return null;
    }

    @Override
    public void enrichNestedSort(NestedSortBuilder sort) {

    }

    @Override
    public QueryBuilder asBuilder() {
        return EsqlDataTypes.isSpatialGeo(dataType) ? new GeoShapeQueryBuilder() : new CartesianShapeQueryBuilder();
    }

    @Override
    protected String innerToString() {
        throw new IllegalArgumentException("SpatialRelatesQuery.innerToString() not implemented");
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, queryRelation, shape, dataType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SpatialRelatesQuery other = (SpatialRelatesQuery) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(queryRelation, other.queryRelation)
            && Objects.equals(shape, other.shape)
            && Objects.equals(dataType, other.dataType);
    }

    public ShapeRelation shapeRelation() {
        return switch (queryRelation.toString().toLowerCase(Locale.ROOT)) {
            case "intersects" -> ShapeRelation.INTERSECTS;
            case "disjoint" -> ShapeRelation.DISJOINT;
            case "within" -> ShapeRelation.WITHIN;
            case "contains" -> ShapeRelation.CONTAINS;
            default -> throw new QlIllegalArgumentException("Unknown shape relation [{}]", queryRelation);
        };
    }

    /**
     * This class is a minimal implementation of the QueryBuilder interface.
     * We only need the toQuery method, but ESQL makes extensive use of QueryBuilder and trimming that interface down for ESQL only would
     * be a large undertaking.
     * Note that this class is only public for testing in PhysicalPlanOptimizerTests.
     */
    public abstract class ShapeQueryBuilder implements QueryBuilder {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException("Unimplemented: toXContent()");
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            throw new UnsupportedOperationException("Unimplemented: toXContent()");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("Unimplemented: toXContent()");
        }

        @Override
        public org.apache.lucene.search.Query toQuery(SearchExecutionContext context) throws IOException {
            final MappedFieldType fieldType = context.getFieldType(field);
            if (fieldType == null) {
                throw new QueryShardException(context, "failed to find type for field [" + field + "]");
            }
            return buildShapeQuery(context, fieldType);
        }

        abstract org.apache.lucene.search.Query buildShapeQuery(SearchExecutionContext context, MappedFieldType fieldType);

        @Override
        public QueryBuilder queryName(String queryName) {
            throw new UnsupportedOperationException("Unimplemented: String");
        }

        @Override
        public String queryName() {
            throw new UnsupportedOperationException("Unimplemented: queryName");
        }

        @Override
        public float boost() {
            return 0;
        }

        @Override
        public QueryBuilder boost(float boost) {
            throw new UnsupportedOperationException("Unimplemented: float");
        }

        @Override
        public String getName() {
            throw new UnsupportedOperationException("Unimplemented: getName");
        }

        /** Public for testing */
        public String fieldName() {
            return field;
        }

        /** Public for testing */
        public ShapeRelation relation() {
            return shapeRelation();
        }

        /** Public for testing */
        public Geometry shape() {
            return shape;
        }
    }

    private class GeoShapeQueryBuilder extends ShapeQueryBuilder {
        public final String NAME = "geo_shape";

        @Override
        public String getWriteableName() {
            return "GeoShapeQueryBuilder";
        }

        @Override
        org.apache.lucene.search.Query buildShapeQuery(SearchExecutionContext context, MappedFieldType fieldType) {
            if ((fieldType instanceof GeoShapeQueryable) == false) {
                throw new QueryShardException(
                    context,
                    "Field [" + field + "] is of unsupported type [" + fieldType.typeName() + "] for [" + NAME + "] query"
                );
            }
            final GeoShapeQueryable ft = (GeoShapeQueryable) fieldType;
            return new ConstantScoreQuery(ft.geoShapeQuery(context, fieldType.name(), shapeRelation(), shape));
        }
    }

    private class CartesianShapeQueryBuilder extends ShapeQueryBuilder {
        @Override
        public String getWriteableName() {
            return "CartesianShapeQueryBuilder";
        }

        @Override
        org.apache.lucene.search.Query buildShapeQuery(SearchExecutionContext context, MappedFieldType fieldType) {
            org.apache.lucene.search.Query innerQuery = dataType == CARTESIAN_POINT
                ? pointShapeQuery(shape, fieldType.name(), queryRelation, context)
                : shapeShapeQuery(shape, fieldType.name(), queryRelation, context);
            return new ConstantScoreQuery(innerQuery);
        }

        /**
         * This code is based on the ShapeQueryPointProcessor.shapeQuery() method
         */
        private static org.apache.lucene.search.Query pointShapeQuery(
            Geometry geometry,
            String fieldName,
            ShapeField.QueryRelation relation,
            SearchExecutionContext context
        ) {
            final boolean hasDocValues = context.getFieldType(fieldName).hasDocValues();
            // only the intersects relation is supported for indexed cartesian point types
            if (relation != ShapeField.QueryRelation.INTERSECTS) {
                throw new QueryShardException(context, relation + " query relation not supported for Field [" + fieldName + "].");
            }
            final Consumer<ShapeType> checker = t -> {
                if (t == ShapeType.POINT || t == ShapeType.MULTIPOINT || t == ShapeType.LINESTRING || t == ShapeType.MULTILINESTRING) {
                    throw new QueryShardException(context, "Field [" + fieldName + "] does not support " + t + " queries");
                }
            };
            final XYGeometry[] luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, checker);
            org.apache.lucene.search.Query query = XYPointField.newGeometryQuery(fieldName, luceneGeometries);
            if (hasDocValues) {
                final org.apache.lucene.search.Query queryDocValues = XYDocValuesField.newSlowGeometryQuery(fieldName, luceneGeometries);
                query = new IndexOrDocValuesQuery(query, queryDocValues);
            }
            return query;
        }

        /**
         * This code is based on the ShapeQueryProcessor.shapeQuery() method
         */
        private static org.apache.lucene.search.Query shapeShapeQuery(
            Geometry geometry,
            String fieldName,
            ShapeField.QueryRelation relation,
            SearchExecutionContext context
        ) {
            final boolean hasDocValues = context.getFieldType(fieldName).hasDocValues();
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0);
            if (relation == ShapeField.QueryRelation.CONTAINS && context.indexVersionCreated().before(IndexVersions.V_7_5_0)) {
                throw new QueryShardException(context, relation + " query relation not supported for Field [" + fieldName + "].");
            }
            if (geometry == null || geometry.isEmpty()) {
                return new MatchNoDocsQuery();
            }
            final XYGeometry[] luceneGeometries;
            try {
                luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
            } catch (IllegalArgumentException e) {
                throw new QueryShardException(context, "Exception creating query on Field [" + fieldName + "] " + e.getMessage(), e);
            }
            org.apache.lucene.search.Query query = XYShape.newGeometryQuery(fieldName, relation, luceneGeometries);
            if (hasDocValues) {
                final org.apache.lucene.search.Query queryDocValues = new CartesianShapeDocValuesQuery(
                    fieldName,
                    relation,
                    luceneGeometries
                );
                query = new IndexOrDocValuesQuery(query, queryDocValues);
            }
            return query;
        }
    }
}
