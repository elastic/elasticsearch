/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractGeometryQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeQueryable;

import java.io.IOException;
import java.text.ParseException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Derived {@link AbstractGeometryQueryBuilder} that builds a {@code x, y} Shape Query. It
 * can be applied to any {@link MappedFieldType} that implements {@link ShapeQueryable}.
 *
 * GeoJson and WKT shape definitions are supported
 */
public class ShapeQueryBuilder extends AbstractGeometryQueryBuilder<ShapeQueryBuilder> {
    public static final String NAME = "shape";

    /**
     * Creates a new ShapeQueryBuilder whose Query will be against the given
     * field name using the given Shape
     *
     * @param fieldName
     *            Name of the field that will be queried
     * @param shape
     *            Shape used in the Query
     */
    public ShapeQueryBuilder(String fieldName, Geometry shape) {
        super(fieldName, shape);
    }

    protected ShapeQueryBuilder(String fieldName, Supplier<Geometry> shapeSupplier, String indexedShapeId) {
        super(fieldName, shapeSupplier, indexedShapeId);
    }

    /**
     * Creates a new ShapeQueryBuilder whose Query will be against the given
     * field name and will use the Shape found with the given ID
     *
     * @param fieldName
     *            Name of the field that will be filtered
     * @param indexedShapeId
     *            ID of the indexed Shape that will be used in the Query
     */
    public ShapeQueryBuilder(String fieldName, String indexedShapeId) {
        super(fieldName, indexedShapeId);
    }

    public ShapeQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
    }

    @Override
    protected ShapeQueryBuilder newShapeQueryBuilder(String fieldName, Geometry shape) {
        return new ShapeQueryBuilder(fieldName, shape);
    }

    @Override
    protected ShapeQueryBuilder newShapeQueryBuilder(String fieldName, Supplier<Geometry> shapeSupplier, String indexedShapeId) {
        return new ShapeQueryBuilder(fieldName, shapeSupplier, indexedShapeId);
    }

    @Override
    @SuppressWarnings({ "rawtypes" })
    public Query buildShapeQuery(SearchExecutionContext context, MappedFieldType fieldType) {
        if ((fieldType instanceof ShapeQueryable) == false) {
            throw new QueryShardException(context,
                "Field [" + fieldName + "] is of unsupported type [" + fieldType.typeName() + "] for [" + NAME + "] query");
        }
        final ShapeQueryable ft = (ShapeQueryable) fieldType;
        return new ConstantScoreQuery(ft.shapeQuery(shape, fieldType.name(), relation, context));
    }

    @Override
    public void doShapeQueryXContent(XContentBuilder builder, Params params) throws IOException {
        // noop
    }

    @Override
    protected ShapeQueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return (ShapeQueryBuilder)super.doRewrite(queryRewriteContext);
    }

    @Override
    protected boolean doEquals(ShapeQueryBuilder other) {
        return super.doEquals((AbstractGeometryQueryBuilder)other);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static class ParsedShapeQueryParams extends ParsedGeometryQueryParams {
        private final GeometryParser geometryParser = new GeometryParser(true, true, true);

        @Override
        protected boolean parseXContentField(XContentParser parser) throws IOException {
            if (SHAPE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                try {
                    this.shape = geometryParser.parse(parser);
                } catch (ParseException e) {
                    throw new IOException(e);
                }
                return true;
            }
            return false;
        }
    }

    public static ShapeQueryBuilder fromXContent(XContentParser parser) throws IOException {
        ParsedShapeQueryParams pgsqb = (ParsedShapeQueryParams)AbstractGeometryQueryBuilder.parsedParamsFromXContent(parser,
            new ParsedShapeQueryParams());

        ShapeQueryBuilder builder;

        if (pgsqb.shape != null) {
            builder = new ShapeQueryBuilder(pgsqb.fieldName, pgsqb.shape);
        } else {
            builder = new ShapeQueryBuilder(pgsqb.fieldName, pgsqb.id);
        }
        if (pgsqb.index != null) {
            builder.indexedShapeIndex(pgsqb.index);
        }
        if (pgsqb.shapePath != null) {
            builder.indexedShapePath(pgsqb.shapePath);
        }
        if (pgsqb.shapeRouting != null) {
            builder.indexedShapeRouting(pgsqb.shapeRouting);
        }
        if (pgsqb.relation != null) {
            builder.relation(pgsqb.relation);
        }
        if (pgsqb.queryName != null) {
            builder.queryName(pgsqb.queryName);
        }
        builder.boost(pgsqb.boost);
        builder.ignoreUnmapped(pgsqb.ignoreUnmapped);
        return builder;
    }
}
