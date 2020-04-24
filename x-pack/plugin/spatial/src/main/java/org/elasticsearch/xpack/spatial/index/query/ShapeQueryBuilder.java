/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper.AbstractGeometryFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractGeometryQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Derived {@link AbstractGeometryQueryBuilder} that builds a {@code x, y} Shape Query
 *
 * GeoJson and WKT shape definitions are supported
 */
public class ShapeQueryBuilder extends AbstractGeometryQueryBuilder<ShapeQueryBuilder> {
    public static final String NAME = "shape";

    protected static final List<String> validContentTypes =
        Collections.unmodifiableList(Arrays.asList(ShapeFieldMapper.CONTENT_TYPE, PointFieldMapper.CONTENT_TYPE));

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name using the given Shape
     *
     * @param fieldName
     *            Name of the field that will be queried
     * @param shape
     *            Shape used in the Query
     * @deprecated use {@link #ShapeQueryBuilder(String, Geometry)} instead
     */
    @Deprecated
    @SuppressWarnings({ "rawtypes" })
    protected ShapeQueryBuilder(String fieldName, ShapeBuilder shape) {
        super(fieldName, shape);
    }

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
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
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
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
    protected List<String> validContentTypes(){
        return validContentTypes;
    }

    @Override
    @SuppressWarnings({ "rawtypes" })
    public Query buildShapeQuery(QueryShardContext context, MappedFieldType fieldType) {
        List<String> validContentTypes = validContentTypes();
        if (validContentTypes.contains(fieldType.typeName()) == false) {
            throw new QueryShardException(context,
                "Field [" + fieldName + "] is not of type [" + String.join(" or ", validContentTypes())
                    + "] but of type [" + fieldType.typeName() + "]");
        }

        final AbstractGeometryFieldType ft = (AbstractGeometryFieldType) fieldType;
        return new ConstantScoreQuery(ft.geometryQueryBuilder().process(shape, ft.name(), relation, context));
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
        @Override
        protected boolean parseXContentField(XContentParser parser) throws IOException {
            if (SHAPE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                this.shape = ShapeParser.parse(parser);
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
