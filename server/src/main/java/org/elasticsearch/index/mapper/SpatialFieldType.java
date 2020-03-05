package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.QueryShardContext;

/** Base class for {@link MappedFieldType} implementations that perform
 *  spatial queries */
public abstract class SpatialFieldType extends MappedFieldType {

    public SpatialFieldType() {}

    protected SpatialFieldType(MappedFieldType ref) {
        super(ref);
    }

    public abstract Query spatialQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context);
}
