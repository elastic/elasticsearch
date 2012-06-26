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

    /**
     * Creates a new GeoShapeFilterBuilder whose Filter will be against the
     * given field name
     *
     * @param name Name of the field that will be filtered
     * @param shape Shape used in the filter
     */
    public GeoShapeFilterBuilder(String name, Shape shape) {
        this.name = name;
        this.shape = shape;
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

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoShapeFilterParser.NAME);

        builder.startObject(name);
        builder.field("relation", relation.getRelationName());

        builder.startObject("shape");
        GeoJSONShapeSerializer.serialize(shape, builder);
        builder.endObject();

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
