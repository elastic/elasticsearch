/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.spatial.XYQueriesUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;

public class SpatialRelatesQuery extends Query {
    private final String field;
    private final ShapeRelation queryRelation;
    private final Geometry shape;
    private final DataType dataType;

    public SpatialRelatesQuery(Source source, String field, ShapeRelation queryRelation, Geometry shape, DataType dataType) {
        super(source);
        this.field = field;
        this.queryRelation = queryRelation;
        this.shape = shape;
        this.dataType = dataType;
    }

    @Override
    public QueryBuilder asBuilder() {
        return DataType.isSpatialGeo(dataType) ? new GeoShapeQueryBuilder() : new CartesianShapeQueryBuilder();
    }

    @Override
    protected String innerToString() {
        return "field:" + field + ", dataType:" + dataType + ", queryRelation:" + queryRelation + ", shape:" + shape;
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
        return switch (queryRelation) {
            case INTERSECTS -> ShapeRelation.INTERSECTS;
            case DISJOINT -> ShapeRelation.DISJOINT;
            case WITHIN -> ShapeRelation.WITHIN;
            case CONTAINS -> ShapeRelation.CONTAINS;
        };
    }

    /**
     * This class is a minimal implementation of the QueryBuilder interface.
     * We only need the toQuery method, but ESQL makes extensive use of QueryBuilder and trimming that interface down for ESQL only would
     * be a large undertaking.
     * Note that this class is only public for testing in PhysicalPlanOptimizerTests.
     */
    public abstract class ShapeQueryBuilder implements QueryBuilder {

        protected void doToXContent(String queryName, XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(queryName);
            builder.startObject(field);
            builder.field("relation", queryRelation);
            builder.field("shape");
            GeoJson.toXContent(shape, builder, params);
            builder.endObject();
            builder.endObject();
            builder.endObject();
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

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // Currently used only in testing and debugging
            doToXContent(NAME, builder, params);
            return builder;
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

        private static org.apache.lucene.search.Query pointShapeQuery(
            Geometry geometry,
            String fieldName,
            ShapeRelation relation,
            SearchExecutionContext context
        ) {
            final MappedFieldType fieldType = context.getFieldType(fieldName);
            try {
                return XYQueriesUtils.toXYPointQuery(geometry, fieldName, relation, fieldType.isIndexed(), fieldType.hasDocValues());
            } catch (IllegalArgumentException e) {
                throw new QueryShardException(context, "Exception creating query on Field [" + fieldName + "] " + e.getMessage(), e);
            }
        }

        private static org.apache.lucene.search.Query shapeShapeQuery(
            Geometry geometry,
            String fieldName,
            ShapeRelation relation,
            SearchExecutionContext context
        ) {
            // CONTAINS queries are not supported by VECTOR strategy for indices created before version 7.5.0 (Lucene 8.3.0);
            if (relation == ShapeRelation.CONTAINS && context.indexVersionCreated().before(IndexVersions.V_7_5_0)) {
                throw new QueryShardException(context, relation + " query relation not supported for Field [" + fieldName + "].");
            }
            final MappedFieldType fieldType = context.getFieldType(fieldName);
            try {
                return XYQueriesUtils.toXYShapeQuery(geometry, fieldName, relation, fieldType.isIndexed(), fieldType.hasDocValues());
            } catch (IllegalArgumentException e) {
                throw new QueryShardException(context, "Exception creating query on Field [" + fieldName + "] " + e.getMessage(), e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // Currently used only in testing and debugging
            doToXContent("cartesian_shape", builder, params);
            return builder;
        }

    }
}
