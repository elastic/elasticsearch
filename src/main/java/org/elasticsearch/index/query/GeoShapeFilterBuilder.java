package org.elasticsearch.index.query;

import com.spatial4j.core.shape.Shape;
import org.elasticsearch.common.geo.GeoJSONShapeSerializer;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * {@link FilterBuilder} that builds a GeoShape Filter
 */
public class GeoShapeFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private ShapeRelation relation = ShapeRelation.INTERSECTS;

    private final Shape shape;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    private final String indexedShapeId;
    private final String indexedShapeType;

    private String indexedShapeIndex;
    private String indexedShapeFieldName;

    /**
     * Creates a new GeoShapeFilterBuilder whose Filter will be against the
     * given field name using the given Shape
     *
     * @param name Name of the field that will be filtered
     * @param shape Shape used in the filter
     */
    public GeoShapeFilterBuilder(String name, Shape shape) {
        this(name, shape, null, null);
    }

    /**
     * Creates a new GeoShapeFilterBuilder whose Filter will be against the given field name
     * and will use the Shape found with the given ID in the given type
     *
     * @param name Name of the field that will be filtered
     * @param indexedShapeId ID of the indexed Shape that will be used in the Filter
     * @param indexedShapeType Index type of the indexed Shapes
     */
    public GeoShapeFilterBuilder(String name, String indexedShapeId, String indexedShapeType) {
        this(name, null, indexedShapeId, indexedShapeType);
    }

    private GeoShapeFilterBuilder(String name, Shape shape, String indexedShapeId, String indexedShapeType) {
        this.name = name;
        this.shape = shape;
        this.indexedShapeId = indexedShapeId;
        this.indexedShapeType = indexedShapeType;
    }

    /**
     * Sets the {@link ShapeRelation} that defines how the Shape used in the
     * Filter must relate to indexed Shapes
     *
     * @param relation ShapeRelation used in the filter
     * @return this
     */
    public GeoShapeFilterBuilder relation(ShapeRelation relation) {
        this.relation = relation;
        return this;
    }

    /**
     * Sets whether the filter will be cached.
     *
     * @param cache Whether filter will be cached
     * @return this
     */
    public GeoShapeFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    /**
     * Sets the key used for the filter if it is cached
     *
     * @param cacheKey Key for the Filter if cached
     * @return this
     */
    public GeoShapeFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    /**
     * Sets the name of the filter
     *
     * @param filterName Name of the filter
     * @return this
     */
    public GeoShapeFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Sets the name of the index where the indexed Shape can be found
     *
     * @param indexedShapeIndex Name of the index where the indexed Shape is
     * @return this
     */
    public GeoShapeFilterBuilder indexedShapeIndex(String indexedShapeIndex) {
        this.indexedShapeIndex = indexedShapeIndex;
        return this;
    }

    /**
     * Sets the name of the field in the indexed Shape document that has the Shape itself
     *
     * @param indexedShapeFieldName Name of the field where the Shape itself is defined
     * @return this
     */
    public GeoShapeFilterBuilder indexedShapeFieldName(String indexedShapeFieldName) {
        this.indexedShapeFieldName = indexedShapeFieldName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoShapeFilterParser.NAME);

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

        builder.endObject();

        if (name != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }

        builder.endObject();
    }
}
