package org.elasticsearch.index.query;

import com.spatial4j.core.shape.Shape;
import org.elasticsearch.common.geo.GeoJSONShapeSerializer;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * {@link QueryBuilder} that builds a GeoShape Query
 */
public class GeoShapeQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<GeoShapeQueryBuilder> {

    private final String name;
    private final Shape shape;

    private ShapeRelation relation = ShapeRelation.INTERSECTS;

    private float boost = -1;

    private final String indexedShapeId;
    private final String indexedShapeType;

    private String indexedShapeIndex;
    private String indexedShapeFieldName;

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the
     * given field name using the given Shape
     *
     * @param name Name of the field that will be queried
     * @param shape Shape used in the query
     */
    public GeoShapeQueryBuilder(String name, Shape shape) {
        this(name, shape, null, null);
    }
    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given field name
     * and will use the Shape found with the given ID in the given type
     *
     * @param name Name of the field that will be queried
     * @param indexedShapeId ID of the indexed Shape that will be used in the Query
     * @param indexedShapeType Index type of the indexed Shapes
     */
    public GeoShapeQueryBuilder(String name, String indexedShapeId, String indexedShapeType) {
        this(name, null, indexedShapeId, indexedShapeType);
    }

    private GeoShapeQueryBuilder(String name, Shape shape, String indexedShapeId, String indexedShapeType) {
        this.name = name;
        this.shape = shape;
        this.indexedShapeId = indexedShapeId;
        this.indexedShapeType = indexedShapeType;
    }

    /**
     * Sets the {@link ShapeRelation} that defines how the Shape used in the
     * Query must relate to indexed Shapes
     *
     * @param relation ShapeRelation used in the filter
     * @return this
     */
    public GeoShapeQueryBuilder relation(ShapeRelation relation) {
        this.relation = relation;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GeoShapeQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the name of the index where the indexed Shape can be found
     *
     * @param indexedShapeIndex Name of the index where the indexed Shape is
     * @return this
     */
    public GeoShapeQueryBuilder indexedShapeIndex(String indexedShapeIndex) {
        this.indexedShapeIndex = indexedShapeIndex;
        return this;
    }

    /**
     * Sets the name of the field in the indexed Shape document that has the Shape itself
     *
     * @param indexedShapeFieldName Name of the field where the Shape itself is defined
     * @return this
     */
    public GeoShapeQueryBuilder indexedShapeFieldName(String indexedShapeFieldName) {
        this.indexedShapeFieldName = indexedShapeFieldName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoShapeQueryParser.NAME);

        builder.startObject(name);
        builder.field("relation", relation.getRelationName());

        if (shape != null) {
            builder.startObject("shape");
            GeoJSONShapeSerializer.serialize(shape, builder);
            builder.endObject();
        } else {
            builder.startObject("indexed_shape")
                    .field("id", indexedShapeId)
                    .field("type", indexedShapeType);
            if (indexedShapeIndex != null) {
                builder.field("index", indexedShapeIndex);
            }
            if (indexedShapeFieldName != null) {
                builder.field("shape_field_name", indexedShapeFieldName);
            }
            builder.endObject();
        }

        if (boost != -1) {
            builder.field("boost", boost);
        }

        builder.endObject();
    }

}
