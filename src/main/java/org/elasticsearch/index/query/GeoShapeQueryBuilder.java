package org.elasticsearch.index.query;

import com.spatial4j.core.shape.Shape;
import org.elasticsearch.common.geo.GeoJSONShapeSerializer;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class GeoShapeQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<GeoShapeQueryBuilder> {

    private final String name;
    private final Shape shape;

    private ShapeRelation relation = ShapeRelation.INTERSECTS;

    private float boost = -1;

    public GeoShapeQueryBuilder(String name, Shape shape) {
        this.name = name;
        this.shape = shape;
    }

    public GeoShapeQueryBuilder relation(ShapeRelation relation) {
        this.relation = relation;
        return this;
    }

    @Override
    public GeoShapeQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoShapeQueryParser.NAME);

        builder.startObject(name);
        builder.field("relation", relation.getRelationName());

        builder.startObject("shape");
        GeoJSONShapeSerializer.serialize(shape, builder);
        builder.endObject();

        if (boost != -1) {
            builder.field("boost", boost);
        }

        builder.endObject();
    }

}
